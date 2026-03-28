import snowflake.connector
from contextlib import contextmanager
from src.utils.config import config
from src.utils.logger import get_logger

log = get_logger(__name__)


@contextmanager
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=config.snowflake_user,
            password=config.snowflake_password,
            account=config.snowflake_account,
            warehouse=config.snowflake_warehouse,
            database=config.snowflake_database,
            schema=config.snowflake_schema,
        )
        log.info("Snowflake connection established successfully")
        yield conn
    except Exception as e:
        log.error(f"Failed to connect to Snowflake: {e}")
        raise 
    finally:
        if conn:
            conn.close()  # ← always closes, even if error occurs
            log.info("Snowflake connection closed")