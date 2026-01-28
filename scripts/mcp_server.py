#!/usr/bin/env python3
"""
DataWarp v3.1 MCP Server

Exposes NHS data to Claude via Model Context Protocol.

Tools:
    list_datasets   - Show available tables with descriptions
    get_schema      - Get column metadata for a table
    query           - Execute SQL query
    get_periods     - Get available periods for a dataset
    get_lineage     - Get data lineage: source, loads, enrichment history
"""
import json
import logging
import os
import sys
from typing import Any, Dict, List

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from datawarp.storage import get_connection
from datawarp.metadata import get_table_metadata
from datawarp.pipeline import list_configs

# Configure logging to stderr (stdout is reserved for MCP protocol)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Create MCP server
app = Server("datawarp-nhs")


def list_datasets(schema: str = 'staging') -> List[Dict]:
    """List all available datasets with descriptions from saved configs."""
    results = []

    # Build mapping from saved configs (table_name -> (config, SheetMapping))
    configs = list_configs()
    config_map = {}
    for cfg in configs:
        for fp in cfg.file_patterns:
            for sm in fp.sheet_mappings:
                config_map[sm.table_name] = (cfg, sm)

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get all tables
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema,))
            tables = [row[0] for row in cur.fetchall()]

            for table in tables:
                # Get row count
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                row_count = cur.fetchone()[0]

                # Check for period column and get distinct periods
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s AND column_name = 'period'
                """, (schema, table))

                periods = []
                if cur.fetchone():
                    cur.execute(f"SELECT DISTINCT period FROM {schema}.{table} ORDER BY period")
                    periods = [row[0] for row in cur.fetchall()]

                # Get description from config or infer
                if table in config_map:
                    cfg, sm = config_map[table]
                    desc = sm.table_description or _infer_table_description(table)
                    grain = sm.grain
                    grain_desc = sm.grain_description
                    has_enriched = any(k != v for k, v in sm.column_mappings.items())
                    result = {
                        'name': table,
                        'description': desc,
                        'grain': grain,
                        'grain_description': grain_desc,
                        'row_count': row_count,
                        'periods': periods,
                        'pipeline_id': cfg.pipeline_id,
                        'publication_name': cfg.name,
                        'landing_page': cfg.landing_page,
                        'has_enriched_columns': has_enriched,
                        'mappings_version': sm.mappings_version,
                    }
                else:
                    result = {
                        'name': table,
                        'description': _infer_table_description(table),
                        'grain': 'unknown',
                        'grain_description': '',
                        'row_count': row_count,
                        'periods': periods,
                        'pipeline_id': None,
                        'publication_name': None,
                        'landing_page': None,
                        'has_enriched_columns': False,
                        'mappings_version': None,
                    }

                results.append(result)

    return results


def get_schema(table_name: str, schema: str = 'staging') -> Dict:
    """Get detailed schema information for a table."""
    configs = list_configs()
    sheet_mapping = None
    parent_config = None
    for cfg in configs:
        for fp in cfg.file_patterns:
            for sm in fp.sheet_mappings:
                if sm.table_name == table_name:
                    sheet_mapping = sm
                    parent_config = cfg
                    break

    metadata = get_table_metadata(table_name, schema)

    reverse_mappings = {}
    if sheet_mapping:
        reverse_mappings = {v: k for k, v in sheet_mapping.column_mappings.items()}

    if sheet_mapping and parent_config:
        metadata['description'] = sheet_mapping.table_description or metadata.get('description', '')
        metadata['grain'] = sheet_mapping.grain
        metadata['grain_description'] = sheet_mapping.grain_description
        metadata['pipeline_id'] = parent_config.pipeline_id
        metadata['publication_name'] = parent_config.name
        metadata['landing_page'] = parent_config.landing_page
        metadata['column_mappings'] = sheet_mapping.column_mappings
        metadata['mappings_version'] = sheet_mapping.mappings_version
        metadata['last_enriched'] = sheet_mapping.last_enriched

        for col in metadata.get('columns', []):
            col_name = col['name']
            original_name = reverse_mappings.get(col_name, col_name)
            col['original_name'] = original_name
            col['is_enriched'] = original_name != col_name
            if col_name in sheet_mapping.column_descriptions:
                col['description'] = sheet_mapping.column_descriptions[col_name]
            elif original_name in sheet_mapping.column_descriptions:
                col['description'] = sheet_mapping.column_descriptions[original_name]
    else:
        metadata['pipeline_id'] = None
        metadata['publication_name'] = None
        metadata['landing_page'] = None
        metadata['column_mappings'] = {}
        metadata['mappings_version'] = None
        metadata['last_enriched'] = None
        for col in metadata.get('columns', []):
            col['original_name'] = col['name']
            col['is_enriched'] = False

    return metadata


def query(sql: str, limit: int = 1000) -> Dict:
    """Execute a SQL query and return results."""
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith('SELECT'):
        return {'error': 'Only SELECT queries are allowed'}

    if 'LIMIT' not in sql_upper:
        sql = f"{sql.rstrip(';')} LIMIT {limit}"

    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                rows_serializable = []
                for row in rows:
                    row_dict = {}
                    for i, val in enumerate(row):
                        if hasattr(val, 'isoformat'):
                            val = val.isoformat()
                        elif isinstance(val, (bytes, bytearray)):
                            val = val.decode('utf-8', errors='replace')
                        row_dict[columns[i]] = val
                    rows_serializable.append(row_dict)

                return {
                    'columns': columns,
                    'rows': rows_serializable,
                    'row_count': len(rows),
                }
            except Exception as e:
                return {'error': str(e)}


def get_periods(table_name: str, schema: str = 'staging') -> List[str]:
    """Get list of available periods for a table."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = 'period'
            """, (schema, table_name))

            if not cur.fetchone():
                return []

            cur.execute(f"SELECT DISTINCT period FROM {schema}.{table_name} ORDER BY period")
            return [row[0] for row in cur.fetchall()]


def get_lineage(table_name: str) -> Dict:
    """Get complete lineage information for a table."""
    configs = list_configs()
    sheet_mapping = None
    parent_config = None
    file_pattern_info = None

    for cfg in configs:
        for fp in cfg.file_patterns:
            for sm in fp.sheet_mappings:
                if sm.table_name == table_name:
                    sheet_mapping = sm
                    parent_config = cfg
                    file_pattern_info = fp
                    break

    if parent_config and sheet_mapping and file_pattern_info:
        source = {
            'pipeline_id': parent_config.pipeline_id,
            'publication': parent_config.name,
            'landing_page': parent_config.landing_page,
            'sheet_name': sheet_mapping.sheet_pattern,
            'file_pattern': file_pattern_info.filename_pattern,
        }

        total_cols = len(sheet_mapping.column_mappings)
        enriched_cols = sum(1 for k, v in sheet_mapping.column_mappings.items() if k != v)
        pending_cols = sum(
            1 for col in sheet_mapping.column_mappings.keys()
            if not sheet_mapping.column_descriptions.get(col)
            and not sheet_mapping.column_descriptions.get(sheet_mapping.column_mappings.get(col, col))
        )

        enrichment = {
            'version': sheet_mapping.mappings_version,
            'last_enriched': sheet_mapping.last_enriched,
            'columns_total': total_cols,
            'columns_enriched': enriched_cols,
            'columns_pending': pending_cols,
        }
    else:
        source = {
            'pipeline_id': None,
            'publication': None,
            'landing_page': None,
            'sheet_name': None,
            'file_pattern': None,
        }
        enrichment = {
            'version': None,
            'last_enriched': None,
            'columns_total': 0,
            'columns_enriched': 0,
            'columns_pending': 0,
        }

    loads = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT period, source_file, sheet_name, rows_loaded, loaded_at
                FROM datawarp.tbl_load_history
                WHERE table_name = %s
                ORDER BY loaded_at DESC
            """, (table_name,))

            for row in cur.fetchall():
                loads.append({
                    'period': row[0],
                    'file': row[1],
                    'sheet': row[2],
                    'rows': row[3],
                    'loaded_at': row[4].isoformat() if row[4] else None,
                })

    return {
        'table_name': table_name,
        'source': source,
        'loads': loads,
        'enrichment': enrichment,
    }


def _infer_table_description(table_name: str) -> str:
    """Infer a description from table name."""
    name = table_name.replace('tbl_', '')
    parts = name.split('_')

    if 'adhd' in parts:
        base = 'ADHD referral data'
    elif 'waiting' in parts:
        base = 'Waiting list data'
    elif 'mental' in parts or 'mh' in parts:
        base = 'Mental health data'
    else:
        base = ' '.join(parts).title() + ' data'

    if 'icb' in parts:
        return f"{base} at ICB level"
    elif 'trust' in parts:
        return f"{base} at Trust level"
    elif 'provider' in parts:
        return f"{base} at provider level"
    elif 'national' in parts:
        return f"{base} at national level"

    return base


# MCP Tool Registration
@app.list_tools()
async def handle_list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="list_datasets",
            description="List all NHS datasets with descriptions, grain, publication source, and enrichment status",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "default": "staging"}
                }
            }
        ),
        Tool(
            name="get_schema",
            description="Get column metadata including original/semantic name mappings, descriptions, and whether columns were LLM-enriched",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "schema": {"type": "string", "default": "staging"}
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="query",
            description="Execute a SQL query against the NHS data",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "limit": {"type": "integer", "default": 1000}
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="get_periods",
            description="Get list of available time periods for a dataset",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"},
                    "schema": {"type": "string", "default": "staging"}
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="get_lineage",
            description="Get complete data lineage: source pipeline, publication, file patterns, load history, and enrichment status",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {"type": "string"}
                },
                "required": ["table_name"]
            }
        ),
    ]


@app.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls."""
    logger.info(f"Tool called: {name} with args: {arguments}")

    try:
        if name == "list_datasets":
            result = list_datasets(arguments.get('schema', 'staging'))
        elif name == "get_schema":
            result = get_schema(arguments['table_name'], arguments.get('schema', 'staging'))
        elif name == "query":
            result = query(arguments['sql'], arguments.get('limit', 1000))
        elif name == "get_periods":
            result = get_periods(arguments['table_name'], arguments.get('schema', 'staging'))
        elif name == "get_lineage":
            result = get_lineage(arguments['table_name'])
        else:
            result = {'error': f'Unknown tool: {name}'}

        return [TextContent(
            type="text",
            text=json.dumps(result, indent=2, default=str)
        )]

    except Exception as e:
        logger.error(f"Error in tool {name}: {e}", exc_info=True)
        return [TextContent(
            type="text",
            text=f"Error: {str(e)}"
        )]


async def main():
    """Main entry point for stdio server."""
    logger.info("DataWarp v3.1 MCP server starting...")

    # Check database connection
    try:
        datasets = list_datasets()
        logger.info(f"Database connected: {len(datasets)} tables available")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

    # Run the stdio server
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


def test_mode():
    """Run in test mode - show what MCP would return."""
    from rich.console import Console
    from rich.table import Table

    console = Console()

    console.print("\n[bold blue]DataWarp MCP Server - Test Mode[/]\n")

    # Test list_datasets
    console.print("[bold]1. list_datasets()[/]")
    datasets = list_datasets()

    if not datasets:
        console.print("[yellow]No datasets found. Run bootstrap first.[/]")
    else:
        table = Table()
        table.add_column("Table")
        table.add_column("Description")
        table.add_column("Rows", justify="right")
        table.add_column("Enriched")
        table.add_column("Version")

        for d in datasets:
            enriched = "[green]Yes[/]" if d.get('has_enriched_columns') else "[dim]No[/]"
            version = str(d.get('mappings_version', '-'))
            table.add_row(d['name'], d['description'][:40], str(d['row_count']), enriched, version)

        console.print(table)

    # Test get_schema for first table
    if datasets:
        first_table = datasets[0]['name']
        console.print(f"\n[bold]2. get_schema('{first_table}')[/]")

        schema_info = get_schema(first_table)

        console.print(f"  [dim]Pipeline: {schema_info.get('pipeline_id', 'N/A')}[/]")
        console.print(f"  [dim]Version: {schema_info.get('mappings_version', 'N/A')}[/]")
        console.print(f"  [dim]Last enriched: {schema_info.get('last_enriched', 'Never')}[/]")

        mappings = schema_info.get('column_mappings', {})
        if mappings:
            enriched_count = sum(1 for k, v in mappings.items() if k != v)
            console.print(f"  [dim]Column mappings: {len(mappings)} total, {enriched_count} enriched[/]")

        table = Table()
        table.add_column("Column")
        table.add_column("Original")
        table.add_column("Enriched")
        table.add_column("Description")

        for col in schema_info['columns'][:10]:
            original = col.get('original_name', col['name'])
            is_enriched = "[green]Yes[/]" if col.get('is_enriched') else "[dim]No[/]"
            desc = (col.get('description', '') or '')[:35]
            table.add_row(col['name'], original, is_enriched, desc)

        if len(schema_info['columns']) > 10:
            table.add_row('...', '', '', f"({len(schema_info['columns']) - 10} more)")

        console.print(table)

    # Test lineage
    if datasets:
        console.print(f"\n[bold]3. get_lineage('{first_table}')[/]")
        lineage = get_lineage(first_table)

        source = lineage.get('source', {})
        console.print(f"  [dim]Pipeline: {source.get('pipeline_id', 'N/A')}[/]")
        console.print(f"  [dim]Publication: {source.get('publication', 'N/A')}[/]")
        console.print(f"  [dim]Sheet: {source.get('sheet_name', 'N/A')}[/]")
        console.print(f"  [dim]File pattern: {source.get('file_pattern', 'N/A')}[/]")

        enrichment = lineage.get('enrichment', {})
        console.print(f"  [dim]Enrichment: v{enrichment.get('version', '?')}, "
                      f"{enrichment.get('columns_enriched', 0)}/{enrichment.get('columns_total', 0)} enriched, "
                      f"{enrichment.get('columns_pending', 0)} pending[/]")

        loads = lineage.get('loads', [])
        if loads:
            table = Table()
            table.add_column("Period")
            table.add_column("File")
            table.add_column("Rows", justify="right")
            table.add_column("Loaded At")

            for load in loads[:5]:
                loaded_at = load.get('loaded_at', '')
                if loaded_at:
                    loaded_at = loaded_at[:19]
                table.add_row(
                    load.get('period', '-'),
                    (load.get('file', '-') or '-')[:40],
                    str(load.get('rows', 0)),
                    loaded_at
                )

            if len(loads) > 5:
                table.add_row('...', '', '', f"({len(loads) - 5} more)")

            console.print(table)
        else:
            console.print("  [yellow]No load history found[/]")

    console.print("\n[green]MCP server is ready![/]")


if __name__ == '__main__':
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description='DataWarp MCP Server')
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--stdio', action='store_true', help='Run as MCP stdio server')
    args = parser.parse_args()

    if args.test:
        test_mode()
    elif args.stdio:
        asyncio.run(main())
    else:
        # Default: run as stdio server (for Claude Desktop)
        asyncio.run(main())
