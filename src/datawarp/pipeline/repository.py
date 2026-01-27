"""Pipeline configuration persistence"""
import json
from typing import List, Optional
from datetime import datetime

from ..storage import get_connection
from .config import PipelineConfig


def save_config(config: PipelineConfig) -> None:
    """Save or update a pipeline configuration."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO datawarp.tbl_pipeline_configs (pipeline_id, config, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (pipeline_id) DO UPDATE
                SET config = EXCLUDED.config,
                    updated_at = EXCLUDED.updated_at
            """, (config.pipeline_id, json.dumps(config.to_dict()), datetime.now()))


def load_config(pipeline_id: str) -> Optional[PipelineConfig]:
    """Load a pipeline configuration by ID."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT config FROM datawarp.tbl_pipeline_configs
                WHERE pipeline_id = %s
            """, (pipeline_id,))
            row = cur.fetchone()
            if row:
                return PipelineConfig.from_dict(row[0])
    return None


def list_configs() -> List[PipelineConfig]:
    """List all pipeline configurations."""
    configs = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT config FROM datawarp.tbl_pipeline_configs
                ORDER BY pipeline_id
            """)
            for row in cur.fetchall():
                configs.append(PipelineConfig.from_dict(row[0]))
    return configs


def delete_config(pipeline_id: str) -> bool:
    """Delete a pipeline configuration."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM datawarp.tbl_pipeline_configs
                WHERE pipeline_id = %s
            """, (pipeline_id,))
            return cur.rowcount > 0


def record_load(
    pipeline_id: str,
    period: str,
    table_name: str,
    source_file: str,
    sheet_name: Optional[str],
    rows_loaded: int
) -> None:
    """Record a successful data load."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO datawarp.tbl_load_history
                (pipeline_id, period, table_name, source_file, sheet_name, rows_loaded)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_id, period, table_name, sheet_name)
                DO UPDATE SET
                    rows_loaded = EXCLUDED.rows_loaded,
                    loaded_at = NOW()
            """, (pipeline_id, period, table_name, source_file, sheet_name, rows_loaded))


def get_load_history(pipeline_id: str) -> List[dict]:
    """Get load history for a pipeline."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT period, table_name, source_file, sheet_name, rows_loaded, loaded_at
                FROM datawarp.tbl_load_history
                WHERE pipeline_id = %s
                ORDER BY period DESC, table_name
            """, (pipeline_id,))
            columns = ['period', 'table_name', 'source_file', 'sheet_name', 'rows_loaded', 'loaded_at']
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def get_loaded_periods(pipeline_id: str) -> List[str]:
    """Get list of periods that have been loaded for a pipeline."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT period FROM datawarp.tbl_load_history
                WHERE pipeline_id = %s
                ORDER BY period
            """, (pipeline_id,))
            return [row[0] for row in cur.fetchall()]
