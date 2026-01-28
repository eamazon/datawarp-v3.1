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
- `list` - Show pipelines
- `history --pipeline <ID>` - Show load history

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

## Database Schema (3 Tables)

```sql
-- datawarp schema (config)
tbl_pipeline_configs    -- pipeline_id, config (JSONB), timestamps
tbl_load_history        -- pipeline_id, period, table_name, rows_loaded
tbl_enrichment_log      -- LLM call logging (tokens, cost, timing)

-- staging schema (data)
staging.<table_name>    -- Dynamic tables with _period, _loaded_at columns
```

**Full spec:** `docs/DATABASE_SPEC.md`

---

## Key Architecture Decisions

| Decision | Why |
|----------|-----|
| JSONB config storage | Entire PipelineConfig in one column, no 39-table schema |
| DataFrame is truth | DDL and COPY both use df.columns - prevents drift |
| Grain detection before enrichment | Skip useless sheets (notes, methodology) |
| Append-only loading | `_period` column tracks data across periods |
| Enrichment returns dict | Single application point, no scattered logic |

**Full spec:** `docs/ARCHITECTURE.md`

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
├── ARCHITECTURE.md      # System design, data flow, components
├── DATABASE_SPEC.md     # Schema, tables, columns, indexes
├── USER_GUIDE.md        # CLI usage, examples, troubleshooting
└── tasks/
    └── CURRENT.md       # Active work tracking
```

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

## Red Flags - Stop and Reassess

- Creating new database tables beyond the 3 config tables
- File exceeding 300 lines
- Adding "manager", "factory", "orchestrator" classes
- Tests passing without asserting row counts
- Swallowing exceptions silently
- Database changes in code without updating `sql/schema.sql`

---

## When Stuck

1. Check data loaded: `SELECT COUNT(*) FROM staging.<table>`
2. Check columns match: compare `information_schema.columns` to DataFrame
3. Check period detection: print `file.period` for discovered files
4. Check enrichment logs: `SELECT * FROM datawarp.tbl_enrichment_log ORDER BY created_at DESC`
5. Simplify: remove abstraction, get it working, then refactor
