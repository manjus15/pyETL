# test_connection.py
import pytest
from src.database.connection import DatabaseConnection


def test_database_connection():
    """Test database connectivity"""
    try:
        with DatabaseConnection.get_connection() as conn:
            assert conn is not None
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                assert result[0] == 1
    except Exception as e:
        pytest.fail(f"Database connection failed: {str(e)}")


def test_execute_query():
    """Test query execution"""
    try:
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = 'f_claim_submission'"
        result = DatabaseConnection.execute_query(query)
        assert len(result) > 0
    except Exception as e:
        pytest.fail(f"Query execution failed: {str(e)}")
