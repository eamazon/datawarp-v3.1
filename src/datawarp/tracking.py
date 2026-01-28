"""
CLI run tracking - logs all command executions to database.

Implements eventstore pattern for observability without console output.
Gracefully degrades if database is unavailable.
"""
import json
import os
import socket
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, Optional

from .storage import get_connection


def _get_context() -> Dict[str, str]:
    """Get execution context (hostname, username)."""
    return {
        'hostname': socket.gethostname(),
        'username': os.getenv('USER') or os.getenv('USERNAME') or 'unknown',
    }


def start_run(
    command: str,
    args: Dict[str, Any],
    pipeline_id: Optional[str] = None,
) -> Optional[int]:
    """
    Record the start of a CLI command execution.

    Returns:
        Run ID if successful, None if DB unavailable.
    """
    try:
        ctx = _get_context()
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO datawarp.tbl_cli_runs
                    (pipeline_id, command, args, status, hostname, username)
                    VALUES (%s, %s, %s, 'running', %s, %s)
                    RETURNING id
                """, (
                    pipeline_id,
                    command,
                    json.dumps(args),
                    ctx['hostname'],
                    ctx['username'],
                ))
                return cur.fetchone()[0]
    except Exception:
        # Graceful degradation - don't crash if DB unavailable
        return None


def complete_run(
    run_id: Optional[int],
    result_summary: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Record successful completion of a CLI command.

    Args:
        run_id: ID from start_run (None = no-op)
        result_summary: Optional dict with results (rows loaded, etc.)
    """
    if run_id is None:
        return

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE datawarp.tbl_cli_runs
                    SET status = 'success',
                        ended_at = NOW(),
                        duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
                        result_summary = %s
                    WHERE id = %s
                """, (
                    json.dumps(result_summary) if result_summary else None,
                    run_id,
                ))
    except Exception:
        # Graceful degradation
        pass


def fail_run(
    run_id: Optional[int],
    error_message: str,
    result_summary: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Record failed completion of a CLI command.

    Args:
        run_id: ID from start_run (None = no-op)
        error_message: Error description
        result_summary: Optional partial results before failure
    """
    if run_id is None:
        return

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE datawarp.tbl_cli_runs
                    SET status = 'failed',
                        ended_at = NOW(),
                        duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
                        error_message = %s,
                        result_summary = %s
                    WHERE id = %s
                """, (
                    error_message,
                    json.dumps(result_summary) if result_summary else None,
                    run_id,
                ))
    except Exception:
        # Graceful degradation
        pass


@contextmanager
def track_run(
    command: str,
    args: Dict[str, Any],
    pipeline_id: Optional[str] = None,
) -> Generator[Dict[str, Any], None, None]:
    """
    Context manager for tracking CLI command execution.

    Usage:
        with track_run('bootstrap', {'url': url}, pipeline_id) as tracker:
            # do work...
            tracker['rows_loaded'] = 1000
            tracker['tables_created'] = ['tbl_foo', 'tbl_bar']
        # Automatically records success/failure on exit

    The tracker dict is passed to result_summary on completion.
    """
    run_id = start_run(command, args, pipeline_id)
    tracker: Dict[str, Any] = {}

    try:
        yield tracker
        complete_run(run_id, tracker if tracker else None)
    except Exception as e:
        fail_run(run_id, str(e), tracker if tracker else None)
        raise
