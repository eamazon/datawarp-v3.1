#!/usr/bin/env python3
"""
DataWarp v3.1 MCP Server

Exposes NHS data to Claude via Model Context Protocol.

Tools:
    list_datasets   - Show available tables with descriptions
    get_schema      - Get column metadata for a table
    query           - Execute SQL query
    get_periods     - Get available periods for a dataset
"""
import json
import os
import sys
from typing import Any, Dict, List, Optional

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datawarp.storage import get_connection
from datawarp.metadata import get_table_metadata, get_all_tables_metadata
from datawarp.pipeline import list_configs, load_config


def list_datasets(schema: str = 'staging') -> List[Dict]:
    """
    List all available datasets with descriptions from saved configs.

    Returns list of tables with:
        - name: table name
        - description: from LLM enrichment or heuristic
        - grain: entity level (icb, trust, national)
        - row_count: number of rows
        - periods: list of available periods
        - pipeline_id, publication_name, landing_page: source info
        - has_enriched_columns: whether any columns have semantic names
        - mappings_version: config version number
    """
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
                    # Check if any columns have been enriched (semantic != original)
                    has_enriched = any(k != v for k, v in sm.column_mappings.items())
                    result = {
                        'name': table,
                        'description': desc,
                        'grain': grain,
                        'grain_description': grain_desc,
                        'row_count': row_count,
                        'periods': periods,
                        # NEW: Publication context
                        'pipeline_id': cfg.pipeline_id,
                        'publication_name': cfg.name,
                        'landing_page': cfg.landing_page,
                        # NEW: Enrichment metadata
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
    """
    Get detailed schema information for a table.

    Uses saved column descriptions from LLM enrichment if available.

    Returns:
        - table_name, description, grain, grain_description
        - pipeline_id, publication_name, landing_page (source info)
        - column_mappings: dict of original_name -> semantic_name
        - mappings_version, last_enriched (version tracking)
        - columns: list of {name, type, description, sample_values, original_name, is_enriched}
        - row_count
    """
    # Find config for this table
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

    # Get base metadata
    metadata = get_table_metadata(table_name, schema)

    # Build reverse mapping: semantic_name -> original_name
    reverse_mappings = {}
    if sheet_mapping:
        reverse_mappings = {v: k for k, v in sheet_mapping.column_mappings.items()}

    # Enhance with config descriptions
    if sheet_mapping and parent_config:
        metadata['description'] = sheet_mapping.table_description or metadata.get('description', '')
        metadata['grain'] = sheet_mapping.grain
        metadata['grain_description'] = sheet_mapping.grain_description

        # NEW: Publication context
        metadata['pipeline_id'] = parent_config.pipeline_id
        metadata['publication_name'] = parent_config.name
        metadata['landing_page'] = parent_config.landing_page

        # NEW: Column mappings (original -> semantic)
        metadata['column_mappings'] = sheet_mapping.column_mappings

        # NEW: Version tracking
        metadata['mappings_version'] = sheet_mapping.mappings_version
        metadata['last_enriched'] = sheet_mapping.last_enriched

        # Enhance each column with enrichment metadata
        for col in metadata.get('columns', []):
            col_name = col['name']

            # Find original name (reverse lookup from semantic -> original)
            original_name = reverse_mappings.get(col_name, col_name)
            col['original_name'] = original_name
            col['is_enriched'] = original_name != col_name

            # Get description - try semantic name first, then original
            if col_name in sheet_mapping.column_descriptions:
                col['description'] = sheet_mapping.column_descriptions[col_name]
            elif original_name in sheet_mapping.column_descriptions:
                col['description'] = sheet_mapping.column_descriptions[original_name]
    else:
        # No config - add empty enrichment metadata
        metadata['pipeline_id'] = None
        metadata['publication_name'] = None
        metadata['landing_page'] = None
        metadata['column_mappings'] = {}
        metadata['mappings_version'] = None
        metadata['last_enriched'] = None

        # Mark all columns as not enriched
        for col in metadata.get('columns', []):
            col['original_name'] = col['name']
            col['is_enriched'] = False

    return metadata


def query(sql: str, limit: int = 1000) -> Dict:
    """
    Execute a SQL query and return results.

    Args:
        sql: SQL query (SELECT only for safety)
        limit: Maximum rows to return

    Returns:
        - columns: list of column names
        - rows: list of row tuples
        - row_count: number of rows returned
    """
    # Basic safety check - only allow SELECT
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith('SELECT'):
        return {'error': 'Only SELECT queries are allowed'}

    # Add LIMIT if not present
    if 'LIMIT' not in sql_upper:
        sql = f"{sql.rstrip(';')} LIMIT {limit}"

    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql)
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                # Convert to JSON-serializable format
                rows_serializable = []
                for row in rows:
                    row_dict = {}
                    for i, val in enumerate(row):
                        # Handle non-JSON-serializable types
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
    """
    Get list of available periods for a table.

    Returns empty list if table has no period column.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Check if period column exists
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = 'period'
            """, (schema, table_name))

            if not cur.fetchone():
                return []

            cur.execute(f"SELECT DISTINCT period FROM {schema}.{table_name} ORDER BY period")
            return [row[0] for row in cur.fetchall()]


def _infer_table_description(table_name: str) -> str:
    """Infer a description from table name."""
    # Remove tbl_ prefix
    name = table_name.replace('tbl_', '')

    # Split on underscore
    parts = name.split('_')

    # Known patterns
    if 'adhd' in parts:
        base = 'ADHD referral data'
    elif 'waiting' in parts:
        base = 'Waiting list data'
    elif 'mental' in parts or 'mh' in parts:
        base = 'Mental health data'
    else:
        base = ' '.join(parts).title() + ' data'

    # Add level if present
    if 'icb' in parts:
        return f"{base} at ICB level"
    elif 'trust' in parts:
        return f"{base} at Trust level"
    elif 'provider' in parts:
        return f"{base} at provider level"
    elif 'national' in parts:
        return f"{base} at national level"

    return base


# MCP Protocol Implementation
def handle_mcp_request(request: Dict) -> Dict:
    """Handle an MCP protocol request."""
    method = request.get('method', '')
    params = request.get('params', {})

    if method == 'tools/list':
        return {
            'tools': [
                {
                    'name': 'list_datasets',
                    'description': 'List all NHS datasets with descriptions, grain, publication source, and enrichment status',
                    'inputSchema': {
                        'type': 'object',
                        'properties': {
                            'schema': {'type': 'string', 'default': 'staging'}
                        }
                    }
                },
                {
                    'name': 'get_schema',
                    'description': 'Get column metadata including original/semantic name mappings, descriptions, and whether columns were LLM-enriched',
                    'inputSchema': {
                        'type': 'object',
                        'properties': {
                            'table_name': {'type': 'string'},
                            'schema': {'type': 'string', 'default': 'staging'}
                        },
                        'required': ['table_name']
                    }
                },
                {
                    'name': 'query',
                    'description': 'Execute a SQL query against the NHS data',
                    'inputSchema': {
                        'type': 'object',
                        'properties': {
                            'sql': {'type': 'string'},
                            'limit': {'type': 'integer', 'default': 1000}
                        },
                        'required': ['sql']
                    }
                },
                {
                    'name': 'get_periods',
                    'description': 'Get list of available time periods for a dataset',
                    'inputSchema': {
                        'type': 'object',
                        'properties': {
                            'table_name': {'type': 'string'},
                            'schema': {'type': 'string', 'default': 'staging'}
                        },
                        'required': ['table_name']
                    }
                },
            ]
        }

    elif method == 'tools/call':
        tool_name = params.get('name', '')
        arguments = params.get('arguments', {})

        if tool_name == 'list_datasets':
            result = list_datasets(arguments.get('schema', 'staging'))
        elif tool_name == 'get_schema':
            result = get_schema(arguments['table_name'], arguments.get('schema', 'staging'))
        elif tool_name == 'query':
            result = query(arguments['sql'], arguments.get('limit', 1000))
        elif tool_name == 'get_periods':
            result = get_periods(arguments['table_name'], arguments.get('schema', 'staging'))
        else:
            return {'error': f'Unknown tool: {tool_name}'}

        return {'content': [{'type': 'text', 'text': json.dumps(result, indent=2, default=str)}]}

    return {'error': f'Unknown method: {method}'}


def test_mode():
    """Run in test mode - show what MCP would return."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

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

        # Show metadata summary
        console.print(f"  [dim]Pipeline: {schema_info.get('pipeline_id', 'N/A')}[/]")
        console.print(f"  [dim]Version: {schema_info.get('mappings_version', 'N/A')}[/]")
        console.print(f"  [dim]Last enriched: {schema_info.get('last_enriched', 'Never')}[/]")

        # Show column_mappings if present
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

    # Test query
    if datasets:
        console.print(f"\n[bold]3. query('SELECT * FROM staging.{first_table} LIMIT 3')[/]")
        result = query(f"SELECT * FROM staging.{first_table} LIMIT 3")

        if 'error' in result:
            console.print(f"[red]Error: {result['error']}[/]")
        else:
            console.print(f"Returned {result['row_count']} rows, {len(result['columns'])} columns")
            if result['rows']:
                console.print(json.dumps(result['rows'][0], indent=2, default=str)[:500])

    console.print("\n[green]MCP server is ready![/]")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='DataWarp MCP Server')
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--stdio', action='store_true', help='Run as MCP stdio server')
    args = parser.parse_args()

    if args.test:
        test_mode()
    elif args.stdio:
        # MCP stdio mode - read JSON-RPC from stdin, write to stdout
        import sys
        for line in sys.stdin:
            try:
                request = json.loads(line)
                response = handle_mcp_request(request)
                print(json.dumps(response))
                sys.stdout.flush()
            except json.JSONDecodeError:
                print(json.dumps({'error': 'Invalid JSON'}))
                sys.stdout.flush()
    else:
        # Default: show help
        parser.print_help()


if __name__ == '__main__':
    main()
