#!/usr/bin/env python3
"""
DataWarp MCP Server - Expose NHS data to Claude with metadata.

MVP approach: Heuristic metadata from column names + samples, no LLM required.

MCP Tools exposed:
  - list_datasets: Show available tables with descriptions
  - get_schema: Get column metadata for a dataset
  - query: Run SQL or natural language query
  - get_periods: Show available time periods for a dataset

Usage:
    # Start server (stdio mode for Claude Desktop)
    python scripts/mcp_server.py
    
    # Test locally
    python scripts/mcp_server.py --test
"""

import sys
import json
import logging
import re
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict

sys.path.insert(0, 'src')

from datawarp.storage.connection import get_connection

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

STAGING_SCHEMA = 'staging'
PIPELINES_SCHEMA = 'datawarp'


# =============================================================================
# Heuristic Metadata (No LLM Required)
# =============================================================================

# NHS entity patterns - known code formats
NHS_ENTITIES = {
    'icb': {
        'pattern': r'^Q[A-Z]{2}$',
        'description': 'Integrated Care Board (ICB) - NHS regional commissioning body',
        'column_hints': ['icb_code', 'icb', 'commissioner_code']
    },
    'trust': {
        'pattern': r'^R[A-Z0-9]{2,4}$',
        'description': 'NHS Trust - hospital or community services provider',
        'column_hints': ['trust_code', 'provider_code', 'org_code']
    },
    'gp_practice': {
        'pattern': r'^[A-Z]\d{5}$',
        'description': 'GP Practice - primary care provider',
        'column_hints': ['practice_code', 'gp_code']
    },
    'pcn': {
        'pattern': r'^U[0-9]{5}$',
        'description': 'Primary Care Network - group of GP practices',
        'column_hints': ['pcn_code', 'network_code']
    }
}

# Column name patterns → descriptions
COLUMN_PATTERNS = {
    # Counts and measures
    r'.*_count$': 'Count of {subject}',
    r'.*_total$': 'Total {subject}',
    r'.*_rate$': 'Rate per population (usually per 100,000)',
    r'.*_percentage$|.*_pct$|.*_percent$': 'Percentage value',
    r'.*_ratio$': 'Ratio between two measures',
    
    # Time-related
    r'.*_date$': 'Date value',
    r'.*_month$': 'Month identifier',
    r'.*_year$': 'Year value',
    r'.*_period$|^period$': 'Reporting period (usually YYYY-MM)',
    r'.*_fy$': 'Financial year',
    
    # Entity identifiers
    r'.*_code$': 'Identifier code',
    r'.*_name$': 'Display name',
    r'.*_id$': 'Unique identifier',
    
    # Clinical
    r'.*referral.*': 'Referral to specialist service',
    r'.*waiting.*': 'Waiting time or waiting list metric',
    r'.*appointment.*': 'Appointment booking or attendance',
    r'.*admission.*': 'Hospital admission',
    r'.*discharge.*': 'Hospital discharge',
    r'.*attendance.*': 'A&E or outpatient attendance',
    
    # ADHD-specific
    r'.*first_contact.*': 'First contact with service after referral',
    r'.*diagnosis.*': 'Clinical diagnosis',
    r'.*medication.*|.*prescription.*': 'Medication or prescription',
    r'.*pathway.*': 'Patient pathway stage',
}

# Known canonical column meanings
KNOWN_COLUMNS = {
    'icb_code': 'Integrated Care Board code (e.g., QWE)',
    'icb_name': 'Integrated Care Board name',
    'trust_code': 'NHS Trust code (e.g., RJ1)',
    'trust_name': 'NHS Trust name',
    'provider_code': 'Healthcare provider organisation code',
    'provider_name': 'Healthcare provider organisation name',
    'period': 'Reporting period in YYYY-MM format',
    'financial_year': 'NHS financial year (April to March)',
    'referrals': 'Number of referrals received',
    'referral_count': 'Number of referrals received',
    'first_contact_count': 'Patients receiving first contact with service',
    'waiting_list_size': 'Number of patients on waiting list',
    'median_wait_days': 'Median waiting time in days',
    'incomplete_pathways': 'Patients still waiting for treatment',
    'completed_pathways': 'Patients who completed treatment pathway',
}


def infer_column_description(column_name: str, sample_values: List[Any] = None) -> str:
    """Generate description from column name and samples using heuristics."""
    col_lower = column_name.lower()
    
    # Check known columns first
    if col_lower in KNOWN_COLUMNS:
        return KNOWN_COLUMNS[col_lower]
    
    # Check NHS entity patterns in sample values
    if sample_values:
        str_samples = [str(v) for v in sample_values if v is not None][:10]
        for entity_type, entity_info in NHS_ENTITIES.items():
            if any(re.match(entity_info['pattern'], s) for s in str_samples):
                return entity_info['description']
    
    # Check column name patterns
    for pattern, desc_template in COLUMN_PATTERNS.items():
        if re.match(pattern, col_lower):
            # Extract subject from column name
            subject = col_lower.replace('_count', '').replace('_total', '')
            subject = subject.replace('_', ' ').strip()
            return desc_template.format(subject=subject) if '{subject}' in desc_template else desc_template
    
    # Default: humanize the column name
    humanized = column_name.replace('_', ' ').title()
    return f"{humanized} value"


def infer_table_description(table_name: str, columns: List[str]) -> str:
    """Generate table description from name and columns."""
    # Remove prefix
    name = re.sub(r'^tbl_', '', table_name)
    
    # Detect entity type from columns
    entity_type = None
    for col in columns:
        col_lower = col.lower()
        if 'icb' in col_lower:
            entity_type = 'ICB (Integrated Care Board)'
        elif 'trust' in col_lower or 'provider' in col_lower:
            entity_type = 'Trust/Provider'
        elif 'practice' in col_lower or 'gp' in col_lower:
            entity_type = 'GP Practice'
    
    # Detect subject matter
    subject = None
    name_lower = name.lower()
    if 'adhd' in name_lower:
        subject = 'ADHD services'
    elif 'msa' in name_lower:
        subject = 'Mixed Sex Accommodation breaches'
    elif 'rtt' in name_lower:
        subject = 'Referral to Treatment waiting times'
    elif 'ae' in name_lower or 'emergency' in name_lower:
        subject = 'A&E attendances'
    
    # Build description
    parts = []
    if subject:
        parts.append(subject)
    if entity_type:
        parts.append(f"by {entity_type}")
    
    if parts:
        return ' '.join(parts)
    
    return name.replace('_', ' ').title()


# =============================================================================
# Data Access Layer
# =============================================================================

@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    description: str
    sample_values: List[str]
    null_percentage: float
    distinct_count: int


@dataclass 
class DatasetMetadata:
    table_name: str
    description: str
    row_count: int
    columns: List[ColumnMetadata]
    periods: List[str]
    source_pipeline: Optional[str]


def get_datasets(conn) -> List[Dict]:
    """List all datasets in staging schema."""
    cur = conn.cursor()
    
    # Get tables
    cur.execute(f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{STAGING_SCHEMA}'
          AND table_name NOT LIKE '\\_%'
        ORDER BY table_name
    """)
    
    datasets = []
    for (table_name,) in cur.fetchall():
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{table_name}")
        row_count = cur.fetchone()[0]
        
        # Get columns for description inference
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{STAGING_SCHEMA}' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cur.fetchall()]
        
        # Get periods if available
        periods = []
        if '_period' in columns:
            cur.execute(f"""
                SELECT DISTINCT _period FROM {STAGING_SCHEMA}.{table_name}
                WHERE _period IS NOT NULL
                ORDER BY _period DESC
                LIMIT 24
            """)
            periods = [row[0] for row in cur.fetchall()]
        
        datasets.append({
            'name': table_name,
            'description': infer_table_description(table_name, columns),
            'row_count': row_count,
            'column_count': len([c for c in columns if not c.startswith('_')]),
            'periods': periods[:6],  # Show recent periods
            'period_count': len(periods)
        })
    
    return datasets


def get_dataset_schema(table_name: str, conn) -> Optional[DatasetMetadata]:
    """Get detailed schema for a dataset."""
    cur = conn.cursor()
    
    # Verify table exists
    cur.execute(f"""
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = '{STAGING_SCHEMA}' AND table_name = %s
    """, (table_name,))
    
    if not cur.fetchone():
        return None
    
    # Get row count
    cur.execute(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{table_name}")
    row_count = cur.fetchone()[0]
    
    # Get column details
    cur.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{STAGING_SCHEMA}' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    
    columns = []
    all_col_names = []
    
    for col_name, data_type in cur.fetchall():
        if col_name.startswith('_') and col_name not in ('_period',):
            continue  # Skip internal columns except period
        
        all_col_names.append(col_name)
        
        # Get sample values
        cur.execute(f"""
            SELECT DISTINCT "{col_name}" 
            FROM {STAGING_SCHEMA}.{table_name}
            WHERE "{col_name}" IS NOT NULL
            LIMIT 5
        """)
        samples = [str(row[0])[:50] for row in cur.fetchall()]
        
        # Get null percentage
        cur.execute(f"""
            SELECT 
                COUNT(*) FILTER (WHERE "{col_name}" IS NULL) * 100.0 / NULLIF(COUNT(*), 0),
                COUNT(DISTINCT "{col_name}")
            FROM {STAGING_SCHEMA}.{table_name}
        """)
        null_pct, distinct = cur.fetchone()
        
        columns.append(ColumnMetadata(
            name=col_name,
            data_type=data_type,
            description=infer_column_description(col_name, samples),
            sample_values=samples,
            null_percentage=round(null_pct or 0, 1),
            distinct_count=distinct or 0
        ))
    
    # Get periods
    periods = []
    if '_period' in all_col_names:
        cur.execute(f"""
            SELECT DISTINCT _period FROM {STAGING_SCHEMA}.{table_name}
            WHERE _period IS NOT NULL ORDER BY _period DESC
        """)
        periods = [row[0] for row in cur.fetchall()]
    
    # Get source pipeline
    pipeline = None
    try:
        cur.execute(f"""
            SELECT DISTINCT pipeline_id FROM {PIPELINES_SCHEMA}.tbl_load_history
            WHERE table_name = %s LIMIT 1
        """, (table_name,))
        row = cur.fetchone()
        if row:
            pipeline = row[0]
    except:
        pass
    
    return DatasetMetadata(
        table_name=table_name,
        description=infer_table_description(table_name, all_col_names),
        row_count=row_count,
        columns=columns,
        periods=periods,
        source_pipeline=pipeline
    )


def execute_query(query: str, conn, limit: int = 100) -> Dict:
    """Execute SQL query and return results."""
    cur = conn.cursor()
    
    # Safety: enforce limit
    query_lower = query.lower().strip()
    if 'limit' not in query_lower:
        query = f"{query.rstrip(';')} LIMIT {limit}"
    
    # Only allow SELECT
    if not query_lower.startswith('select'):
        return {'error': 'Only SELECT queries are allowed'}
    
    try:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        return {
            'columns': columns,
            'rows': [list(row) for row in rows],
            'row_count': len(rows),
            'truncated': len(rows) >= limit
        }
    except Exception as e:
        return {'error': str(e)}


def natural_language_to_sql(question: str, table_name: str, schema: DatasetMetadata) -> str:
    """Convert simple natural language to SQL (heuristic, no LLM)."""
    q_lower = question.lower()
    
    # Build column list
    cols = [c.name for c in schema.columns if not c.name.startswith('_') or c.name == '_period']
    
    # Detect aggregation
    agg_keywords = {
        'total': 'SUM',
        'sum': 'SUM',
        'average': 'AVG',
        'mean': 'AVG',
        'count': 'COUNT',
        'how many': 'COUNT',
        'maximum': 'MAX',
        'minimum': 'MIN',
    }
    
    agg_func = None
    for keyword, func in agg_keywords.items():
        if keyword in q_lower:
            agg_func = func
            break
    
    # Detect grouping
    group_col = None
    for col in cols:
        col_words = col.replace('_', ' ').lower()
        if f'by {col_words}' in q_lower or f'per {col_words}' in q_lower:
            group_col = col
            break
    
    # Detect measure column
    measure_col = None
    for col in cols:
        col_words = col.replace('_', ' ').lower()
        if col_words in q_lower:
            measure_col = col
            break
    
    # Detect period filter
    period_filter = None
    period_match = re.search(r'(\d{4}-\d{2})', question)
    if period_match:
        period_filter = period_match.group(1)
    elif 'latest' in q_lower and '_period' in cols:
        period_filter = '__LATEST__'
    
    # Build query
    if agg_func and measure_col and group_col:
        sql = f"SELECT {group_col}, {agg_func}({measure_col}) as {measure_col}_{agg_func.lower()} FROM {STAGING_SCHEMA}.{table_name}"
        if period_filter == '__LATEST__':
            sql += f" WHERE _period = (SELECT MAX(_period) FROM {STAGING_SCHEMA}.{table_name})"
        elif period_filter:
            sql += f" WHERE _period = '{period_filter}'"
        sql += f" GROUP BY {group_col} ORDER BY {agg_func}({measure_col}) DESC LIMIT 20"
        return sql
    
    elif agg_func and measure_col:
        sql = f"SELECT {agg_func}({measure_col}) FROM {STAGING_SCHEMA}.{table_name}"
        if period_filter == '__LATEST__':
            sql += f" WHERE _period = (SELECT MAX(_period) FROM {STAGING_SCHEMA}.{table_name})"
        elif period_filter:
            sql += f" WHERE _period = '{period_filter}'"
        return sql
    
    # Default: show sample rows
    select_cols = cols[:10]  # Limit columns for readability
    sql = f"SELECT {', '.join(select_cols)} FROM {STAGING_SCHEMA}.{table_name}"
    if period_filter == '__LATEST__':
        sql += f" WHERE _period = (SELECT MAX(_period) FROM {STAGING_SCHEMA}.{table_name})"
    elif period_filter:
        sql += f" WHERE _period = '{period_filter}'"
    sql += " LIMIT 10"
    
    return sql


# =============================================================================
# MCP Protocol Implementation
# =============================================================================

def handle_list_tools() -> Dict:
    """Return available MCP tools."""
    return {
        "tools": [
            {
                "name": "list_datasets",
                "description": "List all available NHS datasets with descriptions and row counts",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "get_schema",
                "description": "Get detailed schema and column descriptions for a dataset",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dataset": {
                            "type": "string",
                            "description": "Dataset/table name"
                        }
                    },
                    "required": ["dataset"]
                }
            },
            {
                "name": "query",
                "description": "Query a dataset using SQL or natural language",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dataset": {
                            "type": "string",
                            "description": "Dataset/table name"
                        },
                        "question": {
                            "type": "string",
                            "description": "Natural language question or SQL query"
                        }
                    },
                    "required": ["dataset", "question"]
                }
            },
            {
                "name": "get_periods",
                "description": "Get available time periods for a dataset",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dataset": {
                            "type": "string",
                            "description": "Dataset/table name"
                        }
                    },
                    "required": ["dataset"]
                }
            }
        ]
    }


def handle_call_tool(name: str, arguments: Dict) -> Dict:
    """Execute an MCP tool call."""
    conn = get_connection()
    
    try:
        if name == "list_datasets":
            datasets = get_datasets(conn)
            
            # Format for readability
            lines = ["Available NHS Datasets:\n"]
            for ds in datasets:
                lines.append(f"• {ds['name']}")
                lines.append(f"  {ds['description']}")
                lines.append(f"  {ds['row_count']:,} rows, {ds['column_count']} columns")
                if ds['periods']:
                    lines.append(f"  Periods: {', '.join(ds['periods'][:3])}{'...' if ds['period_count'] > 3 else ''}")
                lines.append("")
            
            return {"content": [{"type": "text", "text": '\n'.join(lines)}]}
        
        elif name == "get_schema":
            dataset = arguments.get('dataset')
            schema = get_dataset_schema(dataset, conn)
            
            if not schema:
                return {"content": [{"type": "text", "text": f"Dataset '{dataset}' not found"}]}
            
            lines = [f"Schema for {schema.table_name}:", f"{schema.description}", f"{schema.row_count:,} rows\n", "Columns:"]
            
            for col in schema.columns:
                lines.append(f"• {col.name} ({col.data_type})")
                lines.append(f"  {col.description}")
                if col.sample_values:
                    lines.append(f"  Samples: {', '.join(col.sample_values[:3])}")
                lines.append("")
            
            if schema.periods:
                lines.append(f"Available periods: {', '.join(schema.periods[:6])}")
            
            return {"content": [{"type": "text", "text": '\n'.join(lines)}]}
        
        elif name == "query":
            dataset = arguments.get('dataset')
            question = arguments.get('question', '')
            
            # Get schema for context
            schema = get_dataset_schema(dataset, conn)
            if not schema:
                return {"content": [{"type": "text", "text": f"Dataset '{dataset}' not found"}]}
            
            # Determine if SQL or natural language
            if question.strip().lower().startswith('select'):
                sql = question
            else:
                sql = natural_language_to_sql(question, dataset, schema)
            
            result = execute_query(sql, conn)
            
            if 'error' in result:
                return {"content": [{"type": "text", "text": f"Query error: {result['error']}\n\nGenerated SQL: {sql}"}]}
            
            # Format results as table
            lines = [f"Query: {sql}\n", f"Results ({result['row_count']} rows):\n"]
            
            # Header
            lines.append(' | '.join(result['columns']))
            lines.append('-' * 60)
            
            # Rows
            for row in result['rows'][:20]:
                lines.append(' | '.join(str(v)[:20] if v is not None else 'NULL' for v in row))
            
            if result['truncated']:
                lines.append(f"\n... (results truncated)")
            
            return {"content": [{"type": "text", "text": '\n'.join(lines)}]}
        
        elif name == "get_periods":
            dataset = arguments.get('dataset')
            schema = get_dataset_schema(dataset, conn)
            
            if not schema:
                return {"content": [{"type": "text", "text": f"Dataset '{dataset}' not found"}]}
            
            if not schema.periods:
                return {"content": [{"type": "text", "text": f"No period data available for {dataset}"}]}
            
            return {"content": [{"type": "text", "text": f"Available periods for {dataset}:\n" + '\n'.join(schema.periods)}]}
        
        else:
            return {"content": [{"type": "text", "text": f"Unknown tool: {name}"}]}
    
    finally:
        conn.close()


def run_mcp_server():
    """Run MCP server in stdio mode."""
    import sys
    
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break
            
            request = json.loads(line)
            method = request.get('method', '')
            
            if method == 'tools/list':
                response = handle_list_tools()
            elif method == 'tools/call':
                params = request.get('params', {})
                response = handle_call_tool(params.get('name'), params.get('arguments', {}))
            else:
                response = {"error": f"Unknown method: {method}"}
            
            response['id'] = request.get('id')
            print(json.dumps(response), flush=True)
            
        except json.JSONDecodeError:
            continue
        except Exception as e:
            print(json.dumps({"error": str(e)}), flush=True)


def run_test():
    """Test MCP tools locally."""
    from rich.console import Console
    from rich.panel import Panel
    
    console = Console()
    conn = get_connection()
    
    try:
        console.print(Panel("[bold cyan]MCP Server Test[/bold cyan]"))
        
        # Test list_datasets
        console.print("\n[bold]1. list_datasets[/bold]")
        datasets = get_datasets(conn)
        for ds in datasets[:3]:
            console.print(f"  • {ds['name']}: {ds['description']} ({ds['row_count']} rows)")
        
        if not datasets:
            console.print("[yellow]No datasets found. Run pipeline.py bootstrap first.[/yellow]")
            return
        
        # Test get_schema
        test_table = datasets[0]['name']
        console.print(f"\n[bold]2. get_schema({test_table})[/bold]")
        schema = get_dataset_schema(test_table, conn)
        if schema:
            for col in schema.columns[:5]:
                console.print(f"  • {col.name}: {col.description}")
        
        # Test query
        console.print(f"\n[bold]3. query({test_table}, 'show latest data')[/bold]")
        if schema:
            sql = natural_language_to_sql('show latest data', test_table, schema)
            console.print(f"  Generated SQL: {sql}")
            result = execute_query(sql, conn, limit=5)
            if 'error' not in result:
                console.print(f"  Returned {result['row_count']} rows")
        
        console.print("\n[green]✓ MCP server ready[/green]")
        
    finally:
        conn.close()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='DataWarp MCP Server')
    parser.add_argument('--test', action='store_true', help='Run local test')
    args = parser.parse_args()
    
    if args.test:
        run_test()
    else:
        run_mcp_server()
