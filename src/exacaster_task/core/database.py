"""
Database Utilities Module.

Provides functions and context managers for interacting with the PostgreSQL database,
including connection management and SQL execution helpers.
"""

import logging
import time
import psycopg2
from psycopg2.extensions import connection as Psycopg2Connection, cursor as Psycopg2Cursor
from typing import Dict, Optional, Any, Generator
from contextlib import contextmanager
import os

logger = logging.getLogger(__name__)

def get_db_params() -> Dict[str, str]:
    """
    Retrieves database connection parameters from environment variables.
    These variables should be loaded by config_settings.py using dotenv.

    Returns:
        Dict[str, str]: A dictionary containing database connection parameters.
    """
    return {
        'dbname': os.getenv('TELCO_DATABASE_NAME', 'telco_database'),
        'user': os.getenv('TELCO_DATABASE_USER', 'postgres'),
        'password': os.getenv('TELCO_DATABASE_PASSWORD', ''),
        'host': os.getenv('TELCO_DATABASE_HOST', 'localhost'),
        'port': os.getenv('TELCO_DATABASE_PORT', '5432')
    }

@contextmanager
def managed_db_connection() -> Generator[Psycopg2Connection, None, None]:
    """
    Provides a database connection as a context manager,
    ensuring it's closed automatically.
    Uses get_db_params() to fetch connection details.

    Yields:
        Psycopg2Connection: An active database connection object.

    Raises:
        Exception: Propagates exceptions from psycopg2.connect or during operations.
    """
    conn: Optional[Psycopg2Connection] = None
    db_params = get_db_params()
    try:
        logger.debug(f"Attempting to connect to database '{db_params['dbname']}'...")
        conn = psycopg2.connect(**db_params)
        logger.debug("Database connection established via context manager.")
        yield conn
    except psycopg2.Error as db_err:
        logger.error(f"Database connection error: {db_err}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed by context manager.")


def log_and_run_sql(
    db_cursor: Psycopg2Cursor,
    log_message: str,
    sql_query: str,
    params: Optional[tuple] = None
) -> None:
    """
    Logs a message and executes an SQL query using the provided database cursor.
    Raises an exception if the query fails.

    Args:
        db_cursor (Psycopg2Cursor): The database cursor for query execution.
        log_message (str): A descriptive message for logging before execution.
        sql_query (str): The SQL query string to execute.
        params (Optional[tuple]): Parameters for the SQL query.
    """
    logger.info(log_message)
    query_start_time = time.time()
    try:
        db_cursor.execute(sql_query, params)
        log_msg_summary = log_message.split('...')[0].strip()
        logger.info(
            f"Query executed successfully in {time.time() - query_start_time:.2f}s. ({log_msg_summary})"
        )
    except Exception as e:
        logger.error(
            f"Failed to execute query: {log_message.split('...')[0].strip()}. Error: {e}",
            exc_info=True
        )
        raise