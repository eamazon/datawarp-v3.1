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
    name = os.getenv('DB_NAME', 'datawalker')
    user = os.getenv('DB_USER', '')
    password = os.getenv('DB_PASSWORD', '')
    host = os.getenv('DB_HOST', '')
    port = os.getenv('DB_PORT', '')

    # Build connection string - only include non-empty values
    # This allows unix socket auth when host is not specified
    parts = [f"dbname={name}"]
    if user:
        parts.append(f"user={user}")
    if password:
        parts.append(f"password={password}")
    if host:
        parts.append(f"host={host}")
    if port:
        parts.append(f"port={port}")

    return " ".join(parts)


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
