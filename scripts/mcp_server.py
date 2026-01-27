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
    """
    results = []

    # Build mapping from saved configs
    configs = list_configs()
    config_map = {}  # table_name -> SheetMapping
    for cfg in configs:
        for fp in cfg.file_patterns:
            for sm in fp.sheet_mappings:
                config_map[sm.table_name] = sm

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
                    sm = config_map[table]
                    desc = sm.table_description or _infer_table_description(table)
                    grain = sm.grain
                    grain_desc = sm.grain_description
                else:
                    desc = _infer_table_description(table)
                    grain = 'unknown'
                    grain_desc = ''

                results.append({
                    'name': table,
                    'description': desc,
                    'grain': grain,
                    'grain_description': grain_desc,
                    'row_count': row_count,
                    'periods': periods,
                })

    return results


def get_schema(table_name: str, schema: str = 'staging') -> Dict:
    """
    Get detailed schema information for a table.

    Uses saved column descriptions from LLM enrichment if available.

    Returns:
        - table_name
        - description
        - grain
        - columns: list of {name, type, description, sample_values}
        - row_count
    """
    # Find config for this table
    configs = list_configs()
    sheet_mapping = None
    for cfg in configs:
        for fp in cfg.file_patterns:
            for sm in fp.sheet_mappings:
                if sm.table_name == table_name:
                    sheet_mapping = sm
                    break

    # Get base metadata
    metadata = get_table_metadata(table_name, schema)

    # Enhance with config descriptions
    if sheet_mapping:
        metadata['description'] = sheet_mapping.table_description or metadata.get('description', '')
        metadata['grain'] = sheet_mapping.grain
        metadata['grain_description'] = sheet_mapping.grain_description

        # Add enriched column descriptions
        for col in metadata.get('columns', []):
            col_name = col['name']
            if col_name in sheet_mapping.column_descriptions:
                col['description'] = sheet_mapping.column_descriptions[col_name]

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
                    'description': 'List all available NHS datasets with descriptions and row counts',
                    'inputSchema': {
                        'type': 'object',
                        'properties': {
                            'schema': {'type': 'string', 'default': 'staging'}
                        }
                    }
                },
                {
                    'name': 'get_schema',
                    'description': 'Get detailed column information for a dataset',
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
        table.add_column("Periods")

        for d in datasets:
            periods_str = f"{len(d['periods'])} periods" if d['periods'] else '-'
            table.add_row(d['name'], d['description'], str(d['row_count']), periods_str)

        console.print(table)

    # Test get_schema for first table
    if datasets:
        first_table = datasets[0]['name']
        console.print(f"\n[bold]2. get_schema('{first_table}')[/]")

        schema_info = get_schema(first_table)
        table = Table()
        table.add_column("Column")
        table.add_column("Type")
        table.add_column("Description")
        table.add_column("Sample")

        for col in schema_info['columns'][:10]:
            sample = str(col['sample_values'][0]) if col['sample_values'] else '-'
            if len(sample) > 30:
                sample = sample[:30] + '...'
            table.add_row(col['name'], col['type'], col['description'], sample)

        if len(schema_info['columns']) > 10:
            table.add_row('...', '', f"({len(schema_info['columns']) - 10} more)", '')

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
