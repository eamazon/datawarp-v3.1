"""PostgreSQL connection management"""
import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extensions import connection as PgConnection
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


def get_connection_string() -> str:
    """Build connection string from environment variables."""
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    name = os.getenv('POSTGRES_DB', 'datawalker')
    user = os.getenv('POSTGRES_USER', 'databot')
    password = os.getenv('POSTGRES_PASSWORD', '')

    return f"host={host} port={port} dbname={name} user={user} password={password}"


@contextmanager
def get_connection() -> Generator[PgConnection, None, None]:
    """
    Get a PostgreSQL connection as a context manager.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    conn = psycopg2.connect(get_connection_string())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def test_connection() -> bool:
    """Test database connectivity."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone()[0] == 1
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
