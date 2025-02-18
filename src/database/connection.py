# src/database/connection.py
import logging
import time
from contextlib import contextmanager
from functools import wraps
from typing import Any, Dict, List, Optional

import psycopg2
from config.config import Config
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


def retry_on_failure(max_attempts: int = 3, delay_seconds: int = 1) -> Any:
    """Decorator to retry database operations on failure.

    Args:
        max_attempts: Maximum number of retry attempts
        delay_seconds: Delay between retries in seconds
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Database operation failed (attempt {attempt + 1}/{max_attempts}): {str(e)}"
                            f" Retrying in {delay_seconds} seconds..."
                        )
                        time.sleep(delay_seconds)
                    continue
                except Exception as e:
                    logger.error(f"Unexpected database error: {str(e)}")
                    raise

            logger.error(
                f"Database operation failed after {max_attempts} attempts: {str(last_exception)}"
            )
            raise last_exception

        return wrapper

    return decorator


class DatabaseConnection:
    """Handles database connections and operations with retry mechanism."""

    @staticmethod
    @contextmanager
    def get_connection():
        """Get a database connection with automatic cleanup.

        Yields:
            psycopg2.connection: Database connection object

        Raises:
            psycopg2.Error: If connection fails
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=Config.DB_CONFIG.host,
                port=Config.DB_CONFIG.port,
                database=Config.DB_CONFIG.database,
                user=Config.DB_CONFIG.user,
                password=Config.DB_CONFIG.password,
                # Additional connection parameters for better reliability
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error closing database connection: {str(e)}")

    @staticmethod
    @retry_on_failure()
    def execute_query(
        query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a database query with retry mechanism.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of dictionaries containing query results

        Raises:
            psycopg2.Error: If query execution fails after retries
        """
        try:
            with DatabaseConnection.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
        except Exception as e:
            logger.error(
                f"Query execution error: {str(e)}\nQuery: {query}\nParams: {params}"
            )
            raise

    @staticmethod
    @retry_on_failure()
    def execute_batch(
        query: str, params_list: List[Dict[str, Any]], batch_size: int = 1000
    ) -> None:
        """Execute a batch operation with retry mechanism.

        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
            batch_size: Number of operations per batch

        Raises:
            psycopg2.Error: If batch execution fails after retries
        """
        try:
            with DatabaseConnection.get_connection() as conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(params_list), batch_size):
                        batch_params = params_list[i : i + batch_size]
                        cursor.executemany(query, batch_params)
                    conn.commit()
        except Exception as e:
            logger.error(f"Batch execution error: {str(e)}\nQuery: {query}")
            raise

    @staticmethod
    def test_connection() -> bool:
        """Test database connection.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with DatabaseConnection.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
