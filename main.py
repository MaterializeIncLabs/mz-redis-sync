import re

import psycopg2
import redis
import yaml
import logging
from typing import Dict, Any, Optional
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class Config:
    def __init__(self, config_file: str):
        self.config = self._load_config(config_file)

        self.materialize = self.MaterializeConfig(self.config['materialize'])
        self.redis = self.RedisConfig(self.config['redis'])
        self.logging = self.LoggingConfig(self.config.get('logging', {}))

        self.logging.setup_logging()

    @staticmethod
    def _load_config(file_path: str) -> Dict[str, Any]:
        """Load and validate configuration from a YAML file."""
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)
        return config

    class MaterializeConfig:
        def __init__(self, config: Dict[str, Any]):
            required_keys = ['host', 'port', 'user', 'password', 'database', 'sql']
            self._validate_keys(config, required_keys)

            self.host = config['host']
            self.port = config['port']
            self.user = config['user']
            self.password = config['password']
            self.database = config['database']
            self.sql = config['sql']

        @staticmethod
        def _validate_keys(config: Dict[str, Any], keys: list) -> None:
            missing_keys = [key for key in keys if key not in config]
            if missing_keys:
                raise KeyError(f"Missing required Materialize configuration: {', '.join(missing_keys)}")

    class RedisConfig:
        def __init__(self, config: Dict[str, Any]):
            required_keys = ['host', 'port', 'db']
            self._validate_keys(config, required_keys)

            self.host = config['host']
            self.port = config['port']
            self.db = config['db']
            self.mz_timestamp_key = config.get('mz_timestamp_key')
            self.key_prefix = config.get('key_prefix', '').rstrip(':')

        @staticmethod
        def _validate_keys(config: Dict[str, Any], keys: list) -> None:
            missing_keys = [key for key in keys if key not in config]
            if missing_keys:
                raise KeyError(f"Missing required Redis configuration: {', '.join(missing_keys)}")

    class LoggingConfig:
        def __init__(self, config: Dict[str, Any]):
            self.level = config.get('level', 'INFO').upper()
            self.format = config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            self.file = config.get('file')

        def setup_logging(self) -> None:
            """Set up the global logger based on the parsed configuration."""
            logging.basicConfig(level=self.level, format=self.format, filename=self.file)
            global logger
            logger = logging.getLogger(__name__)


class RedisClient:
    def __init__(self, config: Config.RedisConfig):
        self.client = redis.StrictRedis(host=config.host, port=config.port, db=config.db, decode_responses=True)
        self.pipeline = self.client.pipeline()
        self.mz_timestamp_key = config.mz_timestamp_key
        self.key_prefix = config.key_prefix

    def get_latest_timestamp(self) -> Optional[int]:
        """Fetch the latest mz_timestamp from Redis."""
        if self.mz_timestamp_key is not None:
            timestamp = self.client.get(self.mz_timestamp_key)
            logger.debug(f"Fetched latest mz_timestamp from Redis: {timestamp}")
            return timestamp
        return None

    def set_latest_timestamp(self, mz_timestamp: int) -> None:
        """Store the latest mz_timestamp in Redis."""
        if self.mz_timestamp_key is not None:
            self.pipeline.set(self.mz_timestamp_key, mz_timestamp)
        self.pipeline.execute()
        self.pipeline = self.client.pipeline()
        logger.info(f"Updated mz_timestamp in Redis: {mz_timestamp}")

    def format_key(self, key: str) -> str:
        """Format the Redis key with the configured prefix."""
        return f"{self.key_prefix}:{key}" if self.key_prefix else key

    def set_cache(self, key: str, value: str) -> None:
        """Set a key-value pair in Redis."""
        redis_key = self.format_key(key)
        self.pipeline.set(redis_key, value)
        logger.debug(f"Set Redis key: {redis_key} = {value}")

    def delete_cache(self, key: str) -> None:
        """Delete a key from Redis."""
        redis_key = self.format_key(key)
        self.pipeline.delete(redis_key)
        logger.debug(f"Deleted Redis key: {redis_key}")


def connect_to_materialize(config: Config.MaterializeConfig) -> psycopg2.extensions.connection:
    """Establish a connection to the Materialize database."""
    conn = psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.database,
        cursor_factory=RealDictCursor,
        application_name="mz-redis-sync",
        # psycopg2 does not make it easy to redirect the welcome
        # notice to our logger. So instead, we disable it and manually
        # log important details.
        options="--welcome_message=off"
    )

    conn.autocommit = True
    return conn


def build_subscribe_statement(sql: str, ts: Optional[int]) -> str:
    """Create a SQL SUBSCRIBE statement with an optional timestamp."""
    if ts:
        return f"DECLARE c CURSOR FOR SUBSCRIBE ({sql}) WITH (PROGRESS) AS OF {ts} ENVELOPE UPSERT (KEY (key))"
    else:
        return f"DECLARE c CURSOR FOR SUBSCRIBE ({sql}) WITH (SNAPSHOT, PROGRESS) ENVELOPE UPSERT (KEY (key))"


def validate_sql_columns(conn: psycopg2.extensions.connection, sql_query: str) -> None:
    """Validate that the SQL query returns exactly two columns named 'key' and 'value' with appropriate types."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM ({sql_query}) WHERE FALSE LIMIT 0")
            colnames = [desc.name for desc in cur.description]
            coltypes = [desc.type_code for desc in cur.description]

            required_columns = {'key', 'value'}
            returned_columns = set(colnames)

            if len(colnames) != 2:
                extra_columns = returned_columns - required_columns
                raise ValueError(
                    f"Expected exactly two columns, 'key' and 'value'. "
                    f"Found {len(colnames)} columns: {', '.join(colnames)}. "
                    f"Extra columns: {', '.join(extra_columns)}"
                )

            missing_columns = required_columns - returned_columns
            if missing_columns:
                raise ValueError(
                    f"Missing required columns: {', '.join(missing_columns)}. "
                    f"Found columns: {', '.join(colnames)}."
                )

            type_errors = []
            for colname, coltype in zip(colnames, coltypes):
                if coltype not in [psycopg2.STRING, psycopg2.NUMBER, psycopg2.BINARY]:
                    type_errors.append(f"'{colname}' has an invalid type: {coltype}")

            if type_errors:
                raise TypeError(f"Column type errors: {', '.join(type_errors)}")

            logger.info("SQL query validation passed: Columns are 'key' and 'value' with valid types.")

    except Exception as e:
        logger.error(f"SQL validation failed: {e}")
        raise


def main():
    config = Config("config.yaml")
    if config.redis.mz_timestamp_key is None:
        logger.warning(
            "No mz_timestamp_key provided. mz-redis-sync will not support graceful restarts, "
            "leading to full data reconsumption on recovery. This mode may increase Redis load "
            "and could miss deletes in certain scenarios."
        )

    redis_client = RedisClient(config.redis)
    logger.info("Connected to Redis.")

    mz_conn = connect_to_materialize(config.materialize)
    with mz_conn.cursor() as cur:
        cur.execute("SELECT mz_environment_id(), current_database(), current_schema(), current_role()")
        metadata = cur.fetchone()
        cur.execute("SHOW CLUSTER")
        cluster = cur.fetchone()
        logger.info(
            f"Connected to Materialize Environment {metadata['mz_environment_id']} "
            f"using role {metadata['current_role']}")
        logger.info(
            f"Using Materialize database {metadata['current_database']} "
            f"and schema {metadata['current_schema']} on cluster {cluster['cluster']}")

    validate_sql_columns(mz_conn, config.materialize.sql)

    starting_timestamp = redis_client.get_latest_timestamp()
    logger.info(f"Latest mz_timestamp from Redis: {starting_timestamp}")

    subscribe = build_subscribe_statement(config.materialize.sql, starting_timestamp)
    logger.info(subscribe)

    with mz_conn.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(subscribe)

        while True:
            try:
                cur.execute("FETCH 100 c")
                for row in cur:
                    mz_timestamp = int(row['mz_timestamp'])
                    if bool(row['mz_progressed']):
                        redis_client.set_latest_timestamp(mz_timestamp)
                    elif row['mz_state'] == 'upsert':
                        redis_client.set_cache(row['key'], row['value'])
                    elif row['mz_state'] == 'delete':
                        redis_client.delete_cache(row['key'])
                    else:
                        raise ValueError(f"Unknown subscribe state {row['mz_state']}")
            except psycopg2.errors.InternalError_ as e:
                pattern = r"Timestamp \(\d+\) is not valid for all inputs:"
                if re.search(pattern, str(e)):
                    raise RuntimeError("mz-redis-sync has been offline for to long :(")
            except Exception as e:
                logger.error(f"Error processing rows: {e}")
                raise


if __name__ == '__main__':
    main()
