# CLAUDE.md - DataWarp v3.1 Project Rules

## Prime Directive

**Get data into PostgreSQL tables that users can query via MCP.** Everything else is secondary.

## The Mistake We Are Not Repeating

The v3 codebase has 27,000 lines across 39 database tables and zero rows loaded. The DDL bug: column names generated separately for DDL and INSERT, causing mismatches. 

**The fix**: pandas DataFrame.columns is the single source of truth. DDL and INSERT both reference the same list.

```python
# CORRECT - cannot drift
df.columns = [sanitized_names]
ddl = f'CREATE TABLE t ({", ".join(df.columns)})'
copy = f'COPY t ({", ".join(df.columns)}) FROM STDIN'

# WRONG - the old way, names generated separately
ddl_cols = generate_ddl_columns(extractor)
insert_cols = generate_insert_columns(mappings)  # different code path = drift
```

## MVP Scope - Nothing More

| In Scope | Out of Scope |
|----------|--------------|
| Bootstrap: URL → select files/sheets → load → save pattern | LLM enrichment |
| Scan: detect new periods → load using pattern | Queue system |
| MCP: list_datasets, get_schema, query | Agent state machine |
| Heuristic metadata from column names | Multi-user support |
| 2 config tables + staging tables | 39-table normalised schema |

## Coding Rules

### 1. One File Per Concern, Under 300 Lines
If a file exceeds 300 lines, split it. No exceptions.

### 2. No Premature Abstraction
```python
# BAD - abstracting before needed
class DataLoaderFactory:
    def create_loader(self, file_type): ...

# GOOD - direct and simple
def load_excel(path, sheet, table, conn): ...
def load_csv(path, table, conn): ...
```

### 3. Fail Fast and Loud
```python
# BAD - swallowing errors
try:
    load_data()
except Exception:
    pass  # silent failure

# GOOD - explicit failure
try:
    load_data()
except Exception as e:
    console.print(f"[red]Load failed: {e}[/red]")
    raise
```

### 4. No Database Writes in Discovery
Discovery (scraping) returns data structures. Loading writes to database. Never mix.

### 5. JSONB Over Normalisation
Pipeline config goes in one JSONB column, not spread across 10 tables.

## Testing Rules

### 1. Test With Real NHS URLs
```python
# REQUIRED - actual integration test
def test_bootstrap_adhd():
    url = "https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd"
    files = scrape_landing_page(url)
    assert len(files) > 0
    assert any(f.period for f in files)  # periods detected
```

### 2. Verify Rows Loaded, Not Just "No Errors"
```python
# BAD - superficial
def test_load():
    load_sheet(path, sheet, table, conn)
    # no assertion!

# GOOD - verify outcome
def test_load():
    success, rows, _ = load_sheet(path, sheet, table, conn)
    assert success
    assert rows > 0
    
    cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
    assert cur.fetchone()[0] == rows
```

### 3. Test the Column Fix Explicitly
```python
def test_columns_match_between_ddl_and_data():
    """The bug that killed v3 - columns must match."""
    success, rows, learned = load_sheet(path, sheet, table, conn)
    
    # Get DDL columns
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'staging'
    """, (table,))
    ddl_cols = {r[0] for r in cur.fetchall()}
    
    # Get actual data columns
    df = pd.read_sql(f"SELECT * FROM staging.{table} LIMIT 1", conn)
    data_cols = set(df.columns)
    
    # They must match (minus _row_id)
    assert data_cols - {'_row_id'} == ddl_cols - {'_row_id'}
```

### 4. Test Scan Loads to Same Table
```python
def test_scan_appends_not_replaces():
    """New periods append to existing table, not recreate."""
    # Load period 1
    load_sheet(file1, sheet, table, "2024-11", {}, conn)
    cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
    count1 = cur.fetchone()[0]
    
    # Load period 2
    load_sheet(file2, sheet, table, "2024-12", {}, conn)
    cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
    count2 = cur.fetchone()[0]
    
    assert count2 > count1  # appended, not replaced
```

## Definition of Done

### Bootstrap Command
- [ ] Scrapes URL successfully
- [ ] Groups files by period
- [ ] Shows latest period by default
- [ ] Loads selected sheets to staging schema
- [ ] Rows > 0 in created tables
- [ ] Saves PipelineConfig to datawarp.tbl_pipeline_configs
- [ ] Column mappings saved correctly

### Scan Command
- [ ] Loads saved PipelineConfig
- [ ] Detects new periods not in load_history
- [ ] Loads new periods using saved patterns
- [ ] Appends to existing tables (not recreate)
- [ ] Records loads in tbl_load_history

### MCP Server
- [ ] list_datasets returns tables with row counts
- [ ] get_schema returns column descriptions
- [ ] query executes SQL and returns results
- [ ] Heuristic descriptions are meaningful (not just column names)

## Red Flags - Stop and Reassess

- Creating a new database table beyond the 2 config tables
- File exceeding 300 lines
- Adding a "manager", "factory", "orchestrator", or "coordinator" class
- Writing code that doesn't directly serve bootstrap/scan/mcp
- Tests passing without asserting row counts
- Catching exceptions without re-raising or logging

## Commands to Verify Progress

```bash
# 1. Does discovery work?
python -c "from datawarp.discovery import scrape_landing_page; print(len(scrape_landing_page('https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd')))"

# 2. Does bootstrap create tables with data?
python scripts/pipeline.py bootstrap --url "https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd"
psql -c "SELECT table_name, (SELECT COUNT(*) FROM staging.\"' || table_name || '\") FROM information_schema.tables WHERE table_schema='staging'"

# 3. Does scan detect and load new periods?
python scripts/pipeline.py scan --pipeline adhd

# 4. Does MCP return useful metadata?
python scripts/mcp_server.py --test
```

## When Stuck

1. Check if data actually loaded: `SELECT COUNT(*) FROM staging.{table}`
2. Check column names match: compare information_schema.columns to DataFrame.columns
3. Check period detection: print `file.period` for discovered files
4. Simplify: remove abstraction, write direct code, get it working, then refactor
