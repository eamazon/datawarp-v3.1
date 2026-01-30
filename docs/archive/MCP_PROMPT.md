# MCP Server Implementation - Claude Code Prompt

## The v3 Problem

v3 stored metadata in multiple places:
- `tbl_column_aliases` - column name mappings
- `tbl_models` - table info
- `tbl_model_columns` - column info
- Enrichment wrote to one set of tables
- MCP read from different tables
- **Result:** Drift. MCP returned stale or empty descriptions.

## The v3.1 Fix

**ONE source of truth:** `tbl_pipeline_configs.config` (JSONB)

Everything MCP needs is already in the PipelineConfig:
```python
SheetMapping:
    table_name: str                    # "tbl_adhd_icb_referrals"
    table_description: str             # "ADHD referrals by ICB"
    column_mappings: Dict[str, str]    # {"org_code": "icb_code"}
    column_descriptions: Dict[str, str] # {"icb_code": "ICB identifier"}
    grain: str                         # "icb"
    grain_description: str             # "ICB level data"
```

MCP reads from this. No separate metadata tables. No drift.

## MCP Tools to Implement

### 1. list_datasets

Returns all tables with descriptions and grain.

```python
def list_datasets(conn) -> List[dict]:
    """List all datasets from saved pipeline configs."""
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            sm.value->>'table_name' as table_name,
            sm.value->>'table_description' as description,
            sm.value->>'grain' as grain,
            pc.pipeline_id
        FROM datawarp.tbl_pipeline_configs pc,
             jsonb_array_elements(pc.config->'file_patterns') fp,
             jsonb_array_elements(fp->'sheet_mappings') sm
    """)
    
    datasets = []
    for row in cur.fetchall():
        table_name, description, grain, pipeline_id = row
        
        # Get row count from actual table
        try:
            cur.execute(f"SELECT COUNT(*) FROM staging.{table_name}")
            row_count = cur.fetchone()[0]
        except:
            row_count = 0
        
        datasets.append({
            "table_name": table_name,
            "description": description or f"Data from {table_name}",
            "grain": grain,
            "row_count": row_count,
            "pipeline_id": pipeline_id
        })
    
    return datasets
```

### 2. get_schema

Returns column names with descriptions, from config.

```python
def get_schema(table_name: str, conn) -> dict:
    """Get table schema with column descriptions from config."""
    cur = conn.cursor()
    
    # Find this table's config
    cur.execute("""
        SELECT 
            sm.value->>'table_description' as table_description,
            sm.value->>'grain' as grain,
            sm.value->'column_descriptions' as col_descs,
            sm.value->'column_types' as col_types
        FROM datawarp.tbl_pipeline_configs pc,
             jsonb_array_elements(pc.config->'file_patterns') fp,
             jsonb_array_elements(fp->'sheet_mappings') sm
        WHERE sm.value->>'table_name' = %s
        LIMIT 1
    """, (table_name,))
    
    row = cur.fetchone()
    if not row:
        return None
    
    table_description, grain, col_descs, col_types = row
    col_descs = col_descs or {}
    col_types = col_types or {}
    
    # Get actual columns from database (in case schema evolved)
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    
    columns = []
    for col_name, db_type in cur.fetchall():
        if col_name == '_row_id':
            continue
        columns.append({
            "name": col_name,
            "type": col_types.get(col_name, db_type),
            "description": col_descs.get(col_name, "")
        })
    
    return {
        "table_name": table_name,
        "description": table_description,
        "grain": grain,
        "columns": columns
    }
```

### 3. query

Execute SQL and return results.

```python
def query(sql: str, conn, limit: int = 1000) -> dict:
    """Execute SQL query against staging tables."""
    cur = conn.cursor()
    
    # Safety: only allow SELECT
    sql_stripped = sql.strip().lower()
    if not sql_stripped.startswith('select'):
        return {"error": "Only SELECT queries allowed"}
    
    # Add limit if not present
    if 'limit' not in sql_stripped:
        sql = f"{sql.rstrip(';')} LIMIT {limit}"
    
    try:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        return {
            "columns": columns,
            "rows": [list(row) for row in rows],
            "row_count": len(rows)
        }
    except Exception as e:
        return {"error": str(e)}
```

### 4. get_kpis (bonus - for datawarp-nhs style)

Returns numeric columns as queryable metrics.

```python
def get_kpis(table_name: str, conn) -> List[dict]:
    """Get numeric columns (KPIs) for a table."""
    schema = get_schema(table_name, conn)
    if not schema:
        return []
    
    kpis = []
    for col in schema['columns']:
        # Skip metadata and dimension columns
        if col['name'].startswith('_'):
            continue
        if col['name'] in ['period', 'org_code', 'icb_code', 'trust_code']:
            continue
        
        # Include numeric columns
        if col['type'] in ['BIGINT', 'INTEGER', 'DOUBLE PRECISION', 'NUMERIC']:
            kpis.append({
                "name": col['name'],
                "description": col['description'],
                "type": col['type']
            })
    
    return kpis
```

## MCP Server Entry Point

```python
# scripts/mcp_server.py

import json
import sys
from datawarp.storage import get_connection

def handle_request(request: dict) -> dict:
    conn = get_connection()
    
    tool = request.get('tool')
    params = request.get('params', {})
    
    if tool == 'list_datasets':
        return list_datasets(conn)
    
    elif tool == 'get_schema':
        return get_schema(params['table_name'], conn)
    
    elif tool == 'query':
        return query(params['sql'], conn)
    
    elif tool == 'get_kpis':
        return get_kpis(params['table_name'], conn)
    
    else:
        return {"error": f"Unknown tool: {tool}"}


def main():
    # Test mode
    if '--test' in sys.argv:
        conn = get_connection()
        
        print("=== list_datasets ===")
        for ds in list_datasets(conn):
            print(f"  {ds['table_name']}: {ds['description']} ({ds['row_count']} rows, {ds['grain']})")
        
        print("\n=== get_schema (first table) ===")
        datasets = list_datasets(conn)
        if datasets:
            schema = get_schema(datasets[0]['table_name'], conn)
            print(f"  Table: {schema['table_name']}")
            print(f"  Description: {schema['description']}")
            print(f"  Grain: {schema['grain']}")
            print(f"  Columns:")
            for col in schema['columns'][:5]:
                print(f"    - {col['name']}: {col['description']}")
        
        return
    
    # MCP mode: read JSON from stdin, write to stdout
    for line in sys.stdin:
        request = json.loads(line)
        response = handle_request(request)
        print(json.dumps(response))
        sys.stdout.flush()


if __name__ == '__main__':
    main()
```

## What This Enables

User asks Claude: "What ADHD data do you have?"

MCP calls `list_datasets`:
```json
[
  {
    "table_name": "tbl_adhd_table_1",
    "description": "National ADHD referral summary statistics",
    "grain": "national",
    "row_count": 126
  },
  {
    "table_name": "tbl_adhd_table_2a", 
    "description": "ADHD referrals by ICB",
    "grain": "icb",
    "row_count": 8149
  }
]
```

User asks: "Show me the schema for the ICB table"

MCP calls `get_schema`:
```json
{
  "table_name": "tbl_adhd_table_2a",
  "description": "ADHD referrals by ICB",
  "grain": "icb",
  "columns": [
    {"name": "icb_code", "type": "TEXT", "description": "ICB organisation code"},
    {"name": "icb_name", "type": "TEXT", "description": "ICB organisation name"},
    {"name": "referrals_received", "type": "BIGINT", "description": "Number of new referrals"},
    {"name": "first_contacts", "type": "BIGINT", "description": "Number of first contacts made"},
    {"name": "_period", "type": "TEXT", "description": "Reporting period (YYYY-MM)"}
  ]
}
```

Claude can now write intelligent queries because it knows what columns mean.

## Rules

1. **Read from config JSONB** - not separate metadata tables
2. **No drift possible** - descriptions saved at load time, read at query time, same blob
3. **Fallback gracefully** - if description missing, use column name or "No description"
4. **Include grain** - business users need to know if data is ICB/Trust/National level

## Test Command

```bash
python scripts/mcp_server.py --test
```

Should output:
- List of tables with descriptions (not empty strings)
- Schema with column descriptions
- Grain for each table
