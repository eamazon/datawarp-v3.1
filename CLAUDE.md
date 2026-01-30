# CLAUDE.md - DataWarp v3.1

**Read this file at the start of every session.**

---

## What This Project Is

**DataWarp v3.1** is an NHS data pipeline that:
1. Discovers Excel/CSV files from NHS landing pages
2. Detects entity grain (ICB, Trust, GP, Region, National)
3. Enriches with LLM for semantic table/column names
4. Loads to PostgreSQL with period tracking
5. Exposes via MCP for natural language queries

**Ultimate objective:** Enable a chatbot to query NHS data with full context via MCP.

---

## What's Built (Current State)

| Component | Status | Location |
|-----------|--------|----------|
| URL Discovery | ✅ Working | `src/datawarp/discovery/` |
| Period Detection | ✅ Working | `src/datawarp/utils/period.py` |
| Grain Detection | ✅ Working | `src/datawarp/metadata/grain.py` |
| LLM Enrichment | ✅ Working | `src/datawarp/metadata/enrich.py` |
| Data Loading | ✅ Working | `src/datawarp/loader/` |
| Pipeline Config | ✅ Working | `src/datawarp/pipeline/` |
| MCP Server | ✅ Working | `scripts/mcp_server.py` |
| Enrichment Logging | ✅ Working | `datawarp.tbl_enrichment_log` |

**CLI Commands:**
- `bootstrap --url <URL>` - Discover, enrich, load, save pattern
- `scan --pipeline <ID>` - Load new periods automatically
- `backfill --pipeline <ID>` - Load historical periods
- `reset --pipeline <ID>` - Clear data, keep enrichment mappings
- `list` - Show pipelines
- `history --pipeline <ID>` - Show load history

**Full CLI reference:** `docs/mcp/DATAWARP_GUIDE.md` (Section 14)

---

## The V3 Bug We Fixed

V3 had 27,000 lines, 39 tables, and **zero rows loaded**. Column names were generated separately for DDL and INSERT, causing mismatches.

**The fix:** `DataFrame.columns` is the single source of truth.

```python
# CORRECT - cannot drift
df.columns = [sanitized_names]
ddl = f'CREATE TABLE ({", ".join(df.columns)})'
copy = f'COPY ({", ".join(df.columns)}) FROM STDIN'
```

---

## Database Schema (4 Tables, 5 Views)

```sql
-- datawarp schema (config)
tbl_pipeline_configs    -- pipeline_id, config (JSONB), timestamps
tbl_load_history        -- pipeline_id, period, table_name, rows_loaded, source_rows, source_path
tbl_enrichment_log      -- LLM call logging (tokens, cost, timing)
tbl_cli_runs            -- CLI command tracking (timing, status)

-- Views
v_table_metadata        -- Table names, descriptions, grain from config
v_column_metadata       -- Column mappings and descriptions
v_table_stats           -- Row counts, periods loaded per table
v_tables                -- Combined metadata + stats
v_load_reconciliation   -- Source rows vs loaded rows (data integrity check)

-- staging schema (data)
staging.<table_name>    -- Dynamic tables with period column
```

**Source of truth:** `sql/schema.sql`

---

## Key Architecture Decisions

| Decision | Why |
|----------|-----|
| JSONB config storage | Entire PipelineConfig in one column, no 39-table schema |
| DataFrame is truth | DDL and COPY both use df.columns - prevents drift |
| Grain detection before enrichment | Skip useless sheets (notes, methodology) |
| Append-only loading | `period` column tracks data across periods |
| Enrichment returns dict | Single application point, no scattered logic |
| Table name collision prevention | `_TableNameRegistry` ensures unique names per session |
| Source row reconciliation | `v_load_reconciliation` compares source vs loaded rows |
| CSV/XLSX deduplication | ZIP files deduplicated, XLSX preferred over CSV |

**Full guide:** `docs/mcp/DATAWARP_GUIDE.md`

---

## Quick Reference

### Run Bootstrap
```bash
PYTHONPATH=src python scripts/pipeline.py bootstrap \
  --url "https://digital.nhs.uk/.../mi-adhd" \
  --id adhd \
  --enrich
```

### Check Data Loaded
```bash
psql -d datawalker -c "SELECT table_name FROM information_schema.tables WHERE table_schema='staging'"
psql -d datawalker -c "SELECT * FROM datawarp.tbl_load_history ORDER BY loaded_at DESC LIMIT 10"
```

### Verify Data Integrity (no data loss)
```bash
psql -d datawalker -c "SELECT table_name, source_rows, rows_loaded, reconciliation_status FROM datawarp.v_load_reconciliation WHERE reconciliation_status != 'match'"
# Expected: 0 rows
```

### Test MCP
```bash
PYTHONPATH=src python scripts/mcp_server.py --test
```

### Reset Database
```bash
./scripts/reset_db.sh
```

---

## Coding Rules

1. **Under 300 lines per file** - Split if larger
2. **No premature abstraction** - Direct functions over factories
3. **Fail fast and loud** - Log errors, re-raise exceptions
4. **Discovery returns data, loading writes to DB** - Never mix
5. **Test with real NHS URLs** - Assert row counts, not just "no errors"
6. **All DB changes go in `sql/schema.sql`** - This is the single source of truth for database objects. Any new tables, columns, indexes, or views must be added here. Used by `scripts/reset_db.sh` to drop and recreate the entire datawalker database.

---

## Session Workflow

### At Session Start
1. Read this file
2. Check `docs/tasks/CURRENT.md` for pending work
3. Run `git status` to see uncommitted changes

### During Work
- Keep changes focused on one task
- Test with real NHS URLs
- Log enrichment calls for observability

### At Session End
1. Update `docs/tasks/CURRENT.md`
2. Commit changes with clear message
3. Note any blockers or next steps

---

## Documentation Structure

```
docs/
├── mcp/
│   └── DATAWARP_GUIDE.md   # Complete guide (CLI, schema, SQL, architecture)
├── tasks/
│   └── CURRENT.md          # Active work tracking
└── archive/                # Historical design docs (reference only)
```

**Main reference:** `docs/mcp/DATAWARP_GUIDE.md` - covers CLI commands, database schema, SQL verification queries, and architecture.

---

## Environment Setup

**Always use the virtual environment:**
```bash
source .venv/bin/activate
```

```bash
# Required
export POSTGRES_HOST=localhost
export POSTGRES_DB=datawalker
export POSTGRES_USER=databot
export POSTGRES_PASSWORD=databot

# For LLM enrichment
export LLM_PROVIDER=gemini
export GEMINI_API_KEY=your_key
export LLM_MODEL=gemini-2.0-flash-exp
```

---

## Lessons Learned (Code Quality)

These patterns caused problems in this codebase. **Enforce in new code:**

### 1. Check Before You Write
Before writing ANY of these, search if it already exists:
- Type inference → check `loader/excel.py:_infer_pg_type()`
- Column sanitization → use `utils/sanitize.py:sanitize_name()`
- HTTP requests → check `discovery/scraper.py`
- Period parsing → use `utils/period.py:parse_period()`

**Rule:** `grep -r "def <function_name>" src/` before creating new utilities.

### 2. One File, One Purpose
Every file's docstring must state its SINGLE responsibility:
```python
"""Load DataFrames to PostgreSQL. Nothing else."""  # GOOD
"""Handle Excel files and CSVs and downloads and types."""  # BAD - split it
```

**Rule:** If docstring has "and", the file does too much.

### 3. Watch File Growth
When adding to a file:
1. Check current line count: `wc -l <file>`
2. If > 250 lines, consider if new code belongs elsewhere
3. If > 300 lines, MUST split before adding more

**Rule:** Never add features to files already over 250 lines.

### 4. Functions Stay Small
- Max 50 lines per function (excluding docstrings)
- If function has more than 2 levels of nesting, extract helper
- If function has comments like "# Step 1", "# Step 2" → split into functions

### 5. Don't Duplicate, Import
Common duplications to avoid:
| Need | Use | Don't Recreate |
|------|-----|----------------|
| Sanitize column name | `sanitize_name()` | Custom regex |
| Parse period string | `parse_period()` | Date parsing logic |
| DB connection | `get_connection()` | New connection code |
| Console output | `console` from cli | Print statements |

### 6. Keep Working Code Working
- Don't refactor code that works unless you have a specific bug or feature blocked
- Add tests BEFORE refactoring, not after
- "Clean code" that breaks functionality is not clean

---

## Red Flags - Stop and Reassess

- Creating new database tables beyond the 3 config tables
- File exceeding 300 lines
- Adding "manager", "factory", "orchestrator" classes
- Tests passing without asserting row counts
- Swallowing exceptions silently
- Database changes in code without updating `sql/schema.sql`
- Writing a utility function without checking if it exists
- Adding to a file that's already over 250 lines

---

## Existing Utilities (Use These, Don't Recreate)

```python
# Column/name handling
from datawarp.utils.sanitize import sanitize_name, make_table_name

# Period parsing
from datawarp.utils.period import parse_period, normalize_period

# Database
from datawarp.storage import get_connection

# File loading
from datawarp.loader import load_file, load_sheet, load_dataframe, download_file
from datawarp.loader import detect_column_drift  # For schema drift detection
from datawarp.loader.extractor import FileExtractor, get_sheet_names

# Metadata
from datawarp.metadata import detect_grain, enrich_sheet, get_table_metadata

# Pipeline config
from datawarp.pipeline import load_config, save_config, list_configs, record_load

# CLI
from datawarp.cli.console import console  # Rich console for output
```

**Before writing new code, check if these solve your problem.**

---

## When Stuck

1. Check data loaded: `SELECT COUNT(*) FROM staging.<table>`
2. Check columns match: compare `information_schema.columns` to DataFrame
3. Check period detection: print `file.period` for discovered files
4. Check enrichment logs: `SELECT * FROM datawarp.tbl_enrichment_log ORDER BY created_at DESC`
5. Simplify: remove abstraction, get it working, then refactor
