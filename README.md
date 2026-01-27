# DataWarp v3.1 MVP

NHS data pipeline with MCP integration. Simple, working, no over-engineering.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create database (already done if following setup)
createdb datawalker
psql -d datawalker -f sql/schema.sql

# 3. Bootstrap from an NHS URL
python scripts/pipeline.py bootstrap --url "https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd"

# 4. Check what was loaded
python scripts/pipeline.py list
python scripts/pipeline.py history --pipeline mi_adhd

# 5. Later: scan for new data
python scripts/pipeline.py scan --pipeline mi_adhd
```

## Commands

| Command | Description |
|---------|-------------|
| `bootstrap --url <URL>` | Discover files, select sheets, load to DB, save pattern |
| `scan --pipeline <ID>` | Find new periods and load automatically |
| `backfill --pipeline <ID>` | Load all historical periods |
| `list` | Show registered pipelines |
| `history --pipeline <ID>` | Show load history |

## MCP Server

```bash
# Test mode - see what Claude would see
python scripts/mcp_server.py --test

# stdio mode for MCP integration
python scripts/mcp_server.py --stdio
```

## Architecture

```
URL → Discovery → User Selection → Load → Pattern Saved → Auto-scan
        ↓              ↓            ↓           ↓
    scraper.py    CLI prompt    excel.py   repository.py
```

## Database

Only 2 tables:

- `datawarp.tbl_pipeline_configs` - Pipeline patterns as JSONB
- `datawarp.tbl_load_history` - What's been loaded

Data goes to `staging.<table_name>` with a `period` column.

## Key Design Decisions

1. **DataFrame is truth** - Columns sanitized once, used for DDL and COPY
2. **Patterns not code** - File/sheet patterns stored, not hardcoded logic
3. **Heuristic metadata** - No LLM needed for column descriptions
4. **Append-only** - New periods append to existing tables
