# DataWarp v3.1 Database Specification

## Overview

DataWarp uses PostgreSQL with 2 schemas:
- `datawarp` - Configuration and metadata (4 tables, 4 views)
- `staging` - Loaded NHS data (dynamic tables)

---

## Schema: datawarp

### Table: tbl_pipeline_configs

Stores pipeline configuration as JSONB.

| Column | Type | Description |
|--------|------|-------------|
| pipeline_id | VARCHAR(63) PK | Unique identifier (e.g., "mi_adhd") |
| config | JSONB | Complete PipelineConfig object |
| created_at | TIMESTAMP | When pipeline was created |
| updated_at | TIMESTAMP | Last modification |

**Config JSONB Structure:**
```json
{
  "pipeline_id": "mi_adhd",
  "name": "ADHD Referrals",
  "landing_page": "https://digital.nhs.uk/.../mi-adhd",
  "file_patterns": [
    {
      "filename_pattern": "adhd.*\\.xlsx",
      "sheet_mappings": [
        {
          "sheet_pattern": "ICB Data",
          "table_name": "tbl_icb_referrals",
          "table_description": "ADHD referrals by ICB",
          "column_mappings": {"org_code": "icb_code"},
          "column_descriptions": {"icb_code": "ICB identifier"},
          "grain": "icb",
          "grain_column": "icb_code"
        }
      ]
    }
  ],
  "loaded_periods": ["2025-11", "2025-12"]
}
```

---

### Table: tbl_load_history

Tracks what data has been loaded.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Auto-increment ID |
| pipeline_id | VARCHAR(63) | Reference to pipeline |
| period | VARCHAR(20) | Data period (YYYY-MM) |
| table_name | VARCHAR(63) | Target table name |
| source_file | TEXT | Original file URL/path |
| sheet_name | VARCHAR(100) | Sheet name within file |
| rows_loaded | INT | Number of rows loaded |
| loaded_at | TIMESTAMP | When load occurred |

**Indexes:**
- `idx_load_history_pipeline` on (pipeline_id)
- `idx_load_history_period` on (period)

**Unique Constraint:** (pipeline_id, period, table_name, sheet_name)

---

### Table: tbl_enrichment_log

Logs LLM enrichment API calls for observability.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Auto-increment ID |
| pipeline_id | VARCHAR(63) | Reference to pipeline |
| source_file | VARCHAR(255) | File being enriched |
| sheet_name | VARCHAR(100) | Sheet being enriched |
| provider | VARCHAR(50) | LLM provider (gemini, openai) |
| model | VARCHAR(100) | Model name |
| prompt_text | TEXT | Full prompt sent |
| response_text | TEXT | Full response received |
| input_tokens | INT | Tokens in prompt |
| output_tokens | INT | Tokens in response |
| total_tokens | INT | Total tokens used |
| cost_usd | NUMERIC(10,6) | Estimated cost |
| duration_ms | INT | API call duration |
| suggested_table_name | VARCHAR(63) | LLM's suggested name |
| suggested_columns | JSONB | Column mapping suggestions |
| success | BOOLEAN | Whether call succeeded |
| error_message | TEXT | Error if failed |
| original_column_count | INT | Original number of columns before compression |
| compressed_column_count | INT | Number of columns after compression (NULL if not compressed) |
| pattern_detected | VARCHAR(100) | Detected timeseries pattern (e.g., "col_{n}_{n+1}") |
| created_at | TIMESTAMP | When call was made |

**Indexes:**
- `idx_enrichment_log_pipeline` on (pipeline_id)
- `idx_enrichment_log_created` on (created_at DESC)

---

### Table: tbl_cli_runs

Logs all CLI command executions (eventstore pattern).

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Auto-increment ID |
| pipeline_id | VARCHAR(63) | Reference to pipeline (nullable) |
| command | VARCHAR(50) | Command name (bootstrap, scan, backfill, list, history) |
| args | JSONB | Command arguments |
| started_at | TIMESTAMP | When command started |
| ended_at | TIMESTAMP | When command completed |
| duration_ms | INT | Total execution time |
| status | VARCHAR(20) | running, success, failed, cancelled |
| error_message | TEXT | Error details if failed |
| result_summary | JSONB | Flexible summary (rows loaded, files processed, etc.) |
| hostname | VARCHAR(255) | Machine that ran the command |
| username | VARCHAR(100) | User who ran the command |

**Indexes:**
- `idx_cli_runs_pipeline` on (pipeline_id)
- `idx_cli_runs_started` on (started_at DESC)
- `idx_cli_runs_status` on (status)

**Example result_summary:**
```json
{
  "files_discovered": 3,
  "sheets_processed": 8,
  "rows_loaded": 12500,
  "tables_created": ["tbl_icb_data", "tbl_trust_data"]
}
```

---

## Views

### v_table_metadata

Flattens JSONB config into queryable table metadata.

```sql
SELECT * FROM datawarp.v_table_metadata;
```

| Column | Description |
|--------|-------------|
| pipeline_id | Pipeline identifier |
| publication_name | Human-readable name |
| table_name | Staging table name |
| table_description | What the table contains |
| grain | Entity level (icb, trust, gp, national) |
| grain_column | Column containing entity codes |
| column_mappings | JSONB of raw→semantic names |
| column_descriptions | JSONB of column descriptions |

---

### v_column_metadata

One row per column with descriptions.

```sql
SELECT * FROM datawarp.v_column_metadata WHERE table_name = 'tbl_icb_referrals';
```

| Column | Description |
|--------|-------------|
| pipeline_id | Pipeline identifier |
| table_name | Staging table name |
| grain | Entity level |
| column_name | Original column name |
| semantic_name | LLM-suggested name |
| column_description | What the column contains |

---

### v_table_stats

Load statistics per table.

```sql
SELECT * FROM datawarp.v_table_stats;
```

| Column | Description |
|--------|-------------|
| pipeline_id | Pipeline identifier |
| table_name | Staging table name |
| periods_loaded | Count of distinct periods |
| total_rows | Sum of all rows loaded |
| earliest_period | First period loaded |
| latest_period | Most recent period |
| last_loaded | Timestamp of last load |

---

### v_tables

Combined view - metadata + statistics.

```sql
SELECT * FROM datawarp.v_tables;
```

Returns all columns from v_table_metadata joined with v_table_stats.

---

## Schema: staging

Dynamic tables created per dataset. All tables have:

| Column | Type | Description |
|--------|------|-------------|
| _row_id | SERIAL PK | Auto-increment row ID |
| _period | VARCHAR(20) | Data period (YYYY-MM) |
| _loaded_at | TIMESTAMP | When row was loaded |
| ... | TEXT | Data columns (all stored as TEXT) |

**Example:**
```sql
CREATE TABLE staging.tbl_icb_referrals (
    _row_id SERIAL PRIMARY KEY,
    _period VARCHAR(20),
    _loaded_at TIMESTAMP DEFAULT NOW(),
    icb_code TEXT,
    icb_name TEXT,
    referrals TEXT,
    ...
);
```

---

## Common Queries

**List all tables with row counts:**
```sql
SELECT table_name, table_description, grain, total_rows
FROM datawarp.v_tables
ORDER BY table_name;
```

**Get column descriptions for a table:**
```sql
SELECT column_name, column_description
FROM datawarp.v_column_metadata
WHERE table_name = 'tbl_icb_referrals';
```

**Check what periods are loaded:**
```sql
SELECT DISTINCT period FROM datawarp.tbl_load_history
WHERE pipeline_id = 'mi_adhd'
ORDER BY period;
```

**Enrichment cost tracking:**
```sql
SELECT pipeline_id,
       COUNT(*) as calls,
       SUM(input_tokens) as total_input,
       SUM(output_tokens) as total_output,
       SUM(cost_usd) as total_cost
FROM datawarp.tbl_enrichment_log
WHERE success = true
GROUP BY pipeline_id;
```

**Recent CLI runs:**
```sql
SELECT id, pipeline_id, command, status, duration_ms, started_at
FROM datawarp.tbl_cli_runs
ORDER BY started_at DESC
LIMIT 10;
```

**Failed runs with errors:**
```sql
SELECT pipeline_id, command, error_message, started_at
FROM datawarp.tbl_cli_runs
WHERE status = 'failed'
ORDER BY started_at DESC;
```
