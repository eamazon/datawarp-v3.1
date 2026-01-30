# DataWarp v3.1 User Guide

## Quick Start

```bash
# 1. Bootstrap a new pipeline from NHS URL
PYTHONPATH=src python scripts/pipeline.py bootstrap \
  --url "https://digital.nhs.uk/.../mi-adhd" \
  --id adhd \
  --enrich

# 2. Check what was loaded
PYTHONPATH=src python scripts/pipeline.py list
PYTHONPATH=src python scripts/pipeline.py history --pipeline adhd

# 3. Scan for new periods later
PYTHONPATH=src python scripts/pipeline.py scan --pipeline adhd
```

---

## CLI Commands Cheat Sheet

| Command | Description | Example |
|---------|-------------|---------|
| `bootstrap` | Create new pipeline from URL | `bootstrap --url <URL> --id <short_id> --enrich` |
| `scan` | Load new periods for existing pipeline | `scan --pipeline adhd` |
| `backfill` | Load all historical periods | `backfill --pipeline adhd` |
| `list` | Show all pipelines | `list` |
| `history` | Show load history | `history --pipeline adhd` |

### Bootstrap Options

```bash
bootstrap --url <URL>           # NHS landing page URL (required)
          --id <short_id>       # Short pipeline ID (recommended)
          --name <name>         # Human-readable name
          --enrich              # Use LLM for semantic names
          --period <YYYY-MM>    # Specific period to load
```

### Scan Options

```bash
scan --pipeline <id>            # Pipeline to scan (required)
     --dry-run                  # Show what would be loaded
```

---

## Database Cheat Sheet

### Check Data Loaded

```sql
-- All tables with row counts
SELECT table_name, total_rows, latest_period
FROM datawarp.v_tables;

-- Specific table contents
SELECT * FROM staging.tbl_icb_referrals LIMIT 10;

-- Row counts by period
SELECT _period, COUNT(*)
FROM staging.tbl_icb_referrals
GROUP BY _period;
```

### Reset Database

```bash
./scripts/reset_db.sh
```

---

## Metadata Views

### v_tables - All Tables Overview

```sql
SELECT * FROM datawarp.v_tables;
```

Returns:
| Column | Description |
|--------|-------------|
| pipeline_id | Pipeline identifier |
| table_name | Staging table name |
| publication_name | Human-readable publication name |
| table_description | What the table contains |
| grain | Entity level (icb, trust, gp, national) |
| grain_column | Column with entity codes |
| periods_loaded | Number of periods loaded |
| total_rows | Total rows across all periods |
| earliest_period | First period (YYYY-MM) |
| latest_period | Most recent period |
| last_loaded | Timestamp of last load |

**Example:**
```sql
SELECT table_name, table_description, grain, total_rows
FROM datawarp.v_tables
ORDER BY total_rows DESC;
```

---

### v_table_metadata - Table Details

```sql
SELECT * FROM datawarp.v_table_metadata;
```

Returns:
| Column | Description |
|--------|-------------|
| pipeline_id | Pipeline identifier |
| publication_name | Human-readable name |
| table_name | Staging table name |
| table_description | What the table contains |
| grain | Entity level |
| grain_column | Column with entity codes |
| column_mappings | JSONB: raw → semantic names |
| column_descriptions | JSONB: column descriptions |

**Example:**
```sql
SELECT table_name, table_description, grain
FROM datawarp.v_table_metadata
WHERE grain = 'icb';
```

---

### v_column_metadata - Column Descriptions

```sql
SELECT * FROM datawarp.v_column_metadata;
```

Returns:
| Column | Description |
|--------|-------------|
| pipeline_id | Pipeline identifier |
| table_name | Staging table name |
| grain | Entity level |
| column_name | Original column name |
| semantic_name | LLM-suggested semantic name |
| column_description | What the column contains |

**Example:**
```sql
-- Get all columns for a specific table
SELECT column_name, semantic_name, column_description
FROM datawarp.v_column_metadata
WHERE table_name = 'tbl_icb_referrals';

-- Find columns by name pattern
SELECT table_name, column_name, column_description
FROM datawarp.v_column_metadata
WHERE column_name LIKE '%referral%';
```

---

### v_table_stats - Load Statistics

```sql
SELECT * FROM datawarp.v_table_stats;
```

Returns:
| Column | Description |
|--------|-------------|
| pipeline_id | Pipeline identifier |
| table_name | Staging table name |
| periods_loaded | Distinct periods loaded |
| total_rows | Sum of all rows |
| earliest_period | First period |
| latest_period | Most recent period |
| last_loaded | Last load timestamp |

**Example:**
```sql
-- Find tables that need updates
SELECT table_name, latest_period, last_loaded
FROM datawarp.v_table_stats
WHERE latest_period < '2025-12'
ORDER BY latest_period;
```

---

## Enrichment Tracking

```sql
-- Recent enrichment calls
SELECT pipeline_id, sheet_name, suggested_table_name, success, duration_ms
FROM datawarp.tbl_enrichment_log
ORDER BY created_at DESC
LIMIT 10;

-- Token usage by pipeline
SELECT pipeline_id,
       COUNT(*) as calls,
       SUM(input_tokens) as input_tokens,
       SUM(output_tokens) as output_tokens,
       AVG(duration_ms)::int as avg_ms
FROM datawarp.tbl_enrichment_log
WHERE success = true
GROUP BY pipeline_id;

-- Failed enrichments
SELECT pipeline_id, sheet_name, error_message, created_at
FROM datawarp.tbl_enrichment_log
WHERE success = false
ORDER BY created_at DESC;
```

---

## Load History

```sql
-- Recent loads
SELECT pipeline_id, period, table_name, rows_loaded, loaded_at
FROM datawarp.tbl_load_history
ORDER BY loaded_at DESC
LIMIT 20;

-- Loads per pipeline
SELECT pipeline_id, COUNT(*) as loads, SUM(rows_loaded) as total_rows
FROM datawarp.tbl_load_history
GROUP BY pipeline_id;

-- Find gaps in periods
SELECT DISTINCT period
FROM datawarp.tbl_load_history
WHERE pipeline_id = 'adhd'
ORDER BY period;
```

---

## MCP Server

```bash
# Test mode - see what Claude would see
PYTHONPATH=src python scripts/mcp_server.py --test

# Production mode (stdio for MCP)
PYTHONPATH=src python scripts/mcp_server.py --stdio
```

### MCP Tools

| Tool | Purpose |
|------|---------|
| `list_datasets` | Get all tables with descriptions, grain, row counts |
| `get_schema` | Get column metadata for a table |
| `query` | Execute SQL query |
| `get_periods` | Get available periods for a dataset |

---

## Troubleshooting

### No data loaded
```sql
SELECT COUNT(*) FROM staging.<table_name>;
-- If 0, check load history
SELECT * FROM datawarp.tbl_load_history WHERE table_name = '<table_name>';
```

### Column mismatch errors
```sql
-- Compare DDL columns vs expected
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'staging' AND table_name = '<table_name>';
```

### Enrichment failing
```sql
SELECT error_message, created_at
FROM datawarp.tbl_enrichment_log
WHERE success = false
ORDER BY created_at DESC;
```

### Period not detected
```bash
# Check what periods were found
PYTHONPATH=src python -c "
from datawarp.discovery import scrape_landing_page
files = scrape_landing_page('YOUR_URL')
for f in files:
    print(f'{f.period}: {f.filename}')
"
```
