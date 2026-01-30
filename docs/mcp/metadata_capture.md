# Metadata Capture & Enrichment Guide

This guide explains how DataWarp captures, enriches, and maintains metadata for NHS datasets. Written in plain language with examples you can run yourself.

---

## Table of Contents

1. [What is Metadata and Why Does it Matter?](#1-what-is-metadata-and-why-does-it-matter)
2. [The Enrichment Process](#2-the-enrichment-process)
3. [Bootstrap: Your First Load](#3-bootstrap-your-first-load)
4. [Checking What Was Captured](#4-checking-what-was-captured)
5. [Scan: Loading New Periods](#5-scan-loading-new-periods)
6. [Drift Detection: When Columns Change](#6-drift-detection-when-columns-change)
7. [Re-Enrichment: Filling the Gaps](#7-re-enrichment-filling-the-gaps)
8. [Adding New Sheets](#8-adding-new-sheets)
9. [Version Tracking](#9-version-tracking)
10. [Quick Reference](#10-quick-reference)
11. [Multi-Pattern Support & Auto-Detection](#11-multi-pattern-support--auto-detection)
12. [Data Lineage & Provenance](#12-data-lineage--provenance)
13. [MCP Server: Exposing Data to Claude](#13-mcp-server-exposing-data-to-claude)
14. [CLI Commands Reference](#14-cli-commands-reference)
15. [Database Schema](#15-database-schema)
16. [SQL Verification Queries](#16-sql-verification-queries)
17. [Architecture Overview](#17-architecture-overview)

---

## 1. What is Metadata and Why Does it Matter?

### The Problem

NHS data files have column names like:
```
ORG_CODE, MEASURE_1, MEASURE_2, RPT_PRD_END_DT
```

A human analyst might understand these, but:
- Claude (or any AI) doesn't know what they mean
- New team members struggle with cryptic names
- Documentation gets out of sync with actual data

### The Solution: Enriched Metadata

DataWarp uses an LLM to generate:
- **Semantic names**: `ORG_CODE` → `icb_code`
- **Descriptions**: "Integrated Care Board identifier (e.g., QWE)"
- **Table descriptions**: "ADHD referrals by ICB"

This metadata is stored alongside the data and exposed to Claude via MCP.

### What Gets Captured

| Metadata | Example | Purpose |
|----------|---------|---------|
| `table_name` | `tbl_icb_referrals` | Friendly table name |
| `table_description` | "ADHD referrals by ICB" | What the table contains |
| `column_mappings` | `{"org_code": "icb_code"}` | Original → semantic names |
| `column_descriptions` | `{"icb_code": "ICB identifier"}` | What each column means |
| `grain` | `icb` | Entity level (ICB, Trust, GP, National) |
| `mappings_version` | `2` | How many times config was updated |

---

## 2. The Enrichment Process

### How It Works

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Excel/CSV     │────▶│   LLM (Gemini)  │────▶│  Database +     │
│   File          │     │   Enrichment    │     │  Config JSONB   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
   Raw columns            Semantic names          Stored forever
   MEASURE_1              referrals_received      Ready for MCP
```

### What the LLM Sees

The enrichment prompt includes:
- Sheet name
- Column names
- First 3 rows of sample data
- Publication context (e.g., "ADHD referrals")
- Grain hint (e.g., "icb")

### What the LLM Returns

```json
{
  "table_name": "icb_referrals",
  "table_description": "ADHD referrals by Integrated Care Board",
  "columns": {
    "org_code": "icb_code",
    "measure_1": "referrals_received"
  },
  "descriptions": {
    "icb_code": "ICB identifier (e.g., QWE)",
    "referrals_received": "Number of referrals in period"
  }
}
```

---

## 3. Bootstrap: Your First Load

Bootstrap is the initial setup. It discovers files, enriches metadata, and creates a pipeline config.

### Run Bootstrap

```bash
# Basic bootstrap (no enrichment)
python scripts/pipeline.py bootstrap \
  --url "https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd" \
  --id mi_adhd

# With LLM enrichment (recommended)
python scripts/pipeline.py bootstrap \
  --url "https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd" \
  --id mi_adhd \
  --enrich
```

### What Bootstrap Does

1. **Discovers files** on the NHS landing page
2. **Groups by period** (e.g., 2025-05, 2025-08)
3. **Prompts for selection** (which period, which files)
4. **For each file/sheet**:
   - Detects grain (ICB, Trust, GP, National)
   - Calls LLM for enrichment (if `--enrich`)
   - Loads data to staging table
   - Records column mappings and descriptions
5. **Saves pipeline config** as JSONB

### Example Output

```
Classifying URL...
╭───────────────────────────── URL Classification ─────────────────────────────╮
│ ADHD                                                                         │
│ ID: adhd                                                                     │
│ Discovery: template                                                          │
│ Frequency: quarterly                                                         │
╰──────────────────────────────────────────────────────────────────────────────╯

Found 3 files

Processing: adhd_may25.csv
  Grain: icb (Integrated Care Board)
  Enriching with LLM...
  LLM suggested: tbl_icb_referrals
  Loaded 1304 rows to staging.tbl_icb_referrals

╭───────────────────────────── Bootstrap Complete ─────────────────────────────╮
│ Pipeline: mi_adhd                                                            │
│ Files: 3 pattern(s)                                                          │
│ Period: 2025-05                                                              │
╰──────────────────────────────────────────────────────────────────────────────╯
```

---

## 4. Checking What Was Captured

After bootstrap, verify the metadata was captured correctly.

### Check Pipeline Exists

```sql
-- List all pipelines
SELECT pipeline_id,
       config->>'name' as name,
       config->>'landing_page' as url,
       jsonb_array_length(config->'file_patterns') as file_patterns
FROM datawarp.tbl_pipeline_configs;
```

**Example output:**
```
 pipeline_id |   name   |                              url                               | file_patterns
-------------+----------+----------------------------------------------------------------+---------------
 mi_adhd     | ADHD     | https://digital.nhs.uk/.../mi-adhd                             |             3
```

### Check Tables Created

```sql
-- List tables with metadata
SELECT table_name,
       table_description,
       grain,
       mappings_version
FROM datawarp.v_table_metadata
WHERE pipeline_id = 'mi_adhd';
```

**Example output:**
```
        table_name         |          table_description          |  grain   | mappings_version
---------------------------+-------------------------------------+----------+------------------
 tbl_adhd_counts           | ADHD referral counts by category    | unknown  |                1
 tbl_national_adhd_metrics | National ADHD metrics definitions   | national |                1
 tbl_adhd_indicators       | Historical ADHD indicator data      | unknown  |                1
```

### Check Column Mappings

```sql
-- See how columns were renamed
SELECT original_name,
       semantic_name,
       is_enriched,
       LEFT(column_description, 50) as description
FROM datawarp.v_column_metadata
WHERE table_name = 'tbl_adhd_counts';
```

**Example output:**
```
        original_name        |   semantic_name   | is_enriched |                    description
-----------------------------+-------------------+-------------+----------------------------------------------------
 indicator_id                | indicator         | t           | The unique identifier for the ADHD indicator.
 reporting_period_start_date | period_start      | t           | The start date of the reporting period.
 reporting_period_end_date   | period_end        | t           | The end date of the reporting period.
 age_group                   | age_group         | f           | The age range for the data.
 value                       | count             | t           | The number of cases or events for the indicator.
```

### Check Data Loaded

```sql
-- Count rows per period
SELECT period, COUNT(*) as rows
FROM staging.tbl_adhd_counts
GROUP BY period
ORDER BY period;
```

**Example output:**
```
 period  | rows
---------+------
 2025-05 | 1304
```

### Check Enrichment Logs

```sql
-- See LLM calls made during bootstrap
SELECT sheet_name,
       suggested_table_name,
       input_tokens + output_tokens as total_tokens,
       duration_ms,
       success
FROM datawarp.tbl_enrichment_log
WHERE pipeline_id = 'mi_adhd'
ORDER BY created_at DESC;
```

**Example output:**
```
   sheet_name    | suggested_table_name | total_tokens | duration_ms | success
-----------------+----------------------+--------------+-------------+---------
 adhd_may25      | adhd_counts          |         1250 |        2340 | t
 Data dictionary | national_adhd_metrics|          890 |        1890 | t
 mhsds_historic  | adhd_indicators      |         1100 |        2100 | t
```

---

## 5. Scan: Loading New Periods

After bootstrap, use `scan` to load newly published data.

### Run Scan

```bash
python scripts/pipeline.py scan --pipeline mi_adhd
```

### What Scan Does

1. **Discovers new periods** using saved URL pattern
2. **Compares against loaded_periods** in config
3. **For each new period**:
   - Downloads matching files
   - Uses saved column mappings (no new enrichment)
   - Loads data with `_period` column
4. **Updates config** with new loaded periods

### Example Output

```
Scanning: ADHD
Discovery mode: template

  o august-2025: 3 files
  o november-2025: 5 files

Found 2 new period(s): 2025-08, 2025-11

Loading period: 2025-08
  Processing: adhd_aug25.csv
    tbl_adhd_counts: 1318 rows

Loading period: 2025-11
  Processing: adhd_nov25.csv
    tbl_adhd_counts: 8149 rows

Scan complete - loaded 2 period(s)
```

### Verify New Periods Loaded

```sql
-- Check periods by table
SELECT period, COUNT(*) as rows
FROM staging.tbl_adhd_counts
GROUP BY period
ORDER BY period;
```

**Example output:**
```
 period  | rows
---------+------
 2025-05 | 1304
 2025-08 | 1318
 2025-11 | 8149
```

---

## 6. Drift Detection: When Columns Change

Sometimes NHS adds new columns to later publications. DataWarp detects this automatically.

### What is Drift?

**Drift** = columns in the new file that weren't in the original bootstrap.

Example:
- May 2025: `indicator_id, value, age_group`
- Nov 2025: `indicator_id, value, age_group, breakdown, primary_level` ← 2 new columns!

### How Drift Detection Works

During `scan`, DataWarp:
1. Compares incoming columns against saved `column_mappings`
2. Detects new columns not in mappings
3. Adds them with **identity mappings** (no semantic name)
4. Sets empty description (flagged for enrichment)
5. Bumps `mappings_version`
6. Saves updated config

### Drift Detection Output

```
Loading period: 2025-11
  Processing: adhd_nov25.csv
New columns detected: {'breakdown', 'primary_level', 'primary_level_description'}
Mappings version bumped to 2
Missing columns (removed in source): {'age_group'}
    tbl_adhd_counts: 8149 rows
Config updated with new column mappings
```

### Check Drift Was Detected

```sql
-- See which columns have empty descriptions (need enrichment)
SELECT original_name,
       semantic_name,
       is_enriched,
       CASE WHEN column_description = '' THEN '(empty)' ELSE 'has description' END as status,
       mappings_version
FROM datawarp.v_column_metadata
WHERE table_name = 'tbl_adhd_counts';
```

**Example output:**
```
        original_name        |   semantic_name   | is_enriched |     status      | mappings_version
-----------------------------+-------------------+-------------+-----------------+------------------
 indicator_id                | indicator         | t           | has description |                2
 value                       | count             | t           | has description |                2
 breakdown                   | breakdown         | f           | (empty)         |                2
 primary_level               | primary_level     | f           | (empty)         |                2
 primary_level_description   | primary_level_desc| f           | (empty)         |                2
```

Notice:
- `is_enriched = false` for new columns (original = semantic)
- `(empty)` description = needs enrichment
- `mappings_version = 2` (bumped from 1)

---

## 7. Re-Enrichment: Filling the Gaps

After drift detection, use `enrich` to fill in the empty descriptions.

### Check What Needs Enrichment

```bash
# Dry run - see what would be enriched
python scripts/pipeline.py enrich --pipeline mi_adhd --dry-run
```

**Output:**
```
Enriching: ADHD
Pipeline: mi_adhd

tbl_adhd_counts
  Columns needing enrichment: 3
    - breakdown
    - primary_level
    - primary_level_description
  Dry run - skipping LLM call

tbl_national_adhd_metrics: all columns have descriptions
tbl_adhd_indicators: all columns have descriptions

Dry run complete - no changes made
```

### Run Enrichment

```bash
# Enrich all tables with empty descriptions
python scripts/pipeline.py enrich --pipeline mi_adhd

# Or target a specific table
python scripts/pipeline.py enrich --pipeline mi_adhd --table tbl_adhd_counts
```

**Output:**
```
Enriching: ADHD
Pipeline: mi_adhd

tbl_adhd_counts
  Columns needing enrichment: 3
    - breakdown
    - primary_level
    - primary_level_description
  Calling LLM for enrichment...
    ✓ breakdown: A category used for further breakdown of the data...
    ✓ primary_level: The code for the primary geographical or organizat...
    ✓ primary_level_description: The descriptive name of the primary...
  Added 3 descriptions, version → 3

tbl_national_adhd_metrics: all columns have descriptions

✓ Enriched 3 columns across 1 table(s)
Config saved
```

### Verify Enrichment

```sql
-- Check descriptions were filled
SELECT original_name,
       semantic_name,
       is_enriched,
       LEFT(column_description, 50) as description,
       mappings_version
FROM datawarp.v_column_metadata
WHERE table_name = 'tbl_adhd_counts'
  AND original_name IN ('breakdown', 'primary_level', 'primary_level_description');
```

**Example output:**
```
        original_name        |    semantic_name     | is_enriched |                    description                     | mappings_version
-----------------------------+----------------------+-------------+----------------------------------------------------+------------------
 breakdown                   | breakdown_category   | t           | A category used for further breakdown of the data  |                3
 primary_level               | primary_level_code   | t           | The code for the primary geographical or organizat |                3
 primary_level_description   | primary_level_name   | t           | The descriptive name of the primary geographical o |                3
```

Notice:
- `is_enriched = true` now (semantic name differs from original)
- Descriptions filled in
- `mappings_version = 3` (bumped again)

### Force Re-Enrichment

To re-enrich ALL columns (even those with existing descriptions):

```bash
python scripts/pipeline.py enrich --pipeline mi_adhd --force
```

---

## 8. Adding New Sheets

When NHS adds a new sheet to an existing Excel file, use `add-sheet`.

### Check Available Sheets

First, manually check what sheets exist:

```bash
# Python one-liner to list sheets in a file
python -c "
from datawarp.loader import get_sheet_names
print(get_sheet_names('/path/to/file.xlsx'))
"
```

### Add a Sheet

```bash
python scripts/pipeline.py add-sheet \
  --pipeline mi_adhd \
  --sheet "New Summary Data"
```

**Output:**
```
Adding sheet to: ADHD
Sheet: New Summary Data

File pattern: data_dictionary_v1\.1\.xlsx
Discovering files...
Using file: data_dictionary_v1.1.xlsx (period: 2025-11)
✓ Sheet found
Rows: 50, Columns: 8
Grain: national (National aggregate data)
Enriching with LLM...
LLM suggested: tbl_national_summary
✓ Loaded 50 rows

✓ Sheet 'New Summary Data' added to pipeline
Table: tbl_national_summary
Config saved
```

### Verify Sheet Added

```sql
-- Check new table appears in metadata
SELECT table_name, grain, mappings_version
FROM datawarp.v_table_metadata
WHERE pipeline_id = 'mi_adhd';
```

### Add Without Enrichment

```bash
python scripts/pipeline.py add-sheet \
  --pipeline mi_adhd \
  --sheet "Raw Data" \
  --no-enrich
```

---

## 9. Version Tracking

Every time the config changes, `mappings_version` is bumped.

### What Changes the Version

| Action | Version Change |
|--------|----------------|
| Bootstrap | Set to 1 |
| Drift detection (new columns) | +1 |
| Re-enrichment | +1 |
| Add-sheet | New mapping at v1 |

### Check Version History

```sql
-- See version per table
SELECT table_name,
       mappings_version,
       last_enriched,
       config_updated
FROM datawarp.v_table_metadata
WHERE pipeline_id = 'mi_adhd';
```

**Example output:**
```
        table_name         | mappings_version |     last_enriched      |       config_updated
---------------------------+------------------+------------------------+----------------------------
 tbl_adhd_counts           |                3 | 2025-01-28T10:30:00    | 2025-01-28 10:30:15.123456
 tbl_national_adhd_metrics |                1 |                        | 2025-01-28 09:00:00.000000
 tbl_adhd_indicators       |                1 |                        | 2025-01-28 09:00:00.000000
```

### Interpret Versions

- `version = 1, last_enriched = null` → Original bootstrap, never re-enriched
- `version = 2, last_enriched = null` → Drift detected, not yet re-enriched
- `version = 3, last_enriched = timestamp` → Drift detected AND re-enriched

---

## 10. Quick Reference

### CLI Commands

```bash
# Initial setup
python scripts/pipeline.py bootstrap --url "..." --id my_pipeline --enrich

# Load new periods
python scripts/pipeline.py scan --pipeline my_pipeline

# Backfill historical periods
python scripts/pipeline.py backfill --pipeline my_pipeline --from 2023-01 --to 2023-12

# Force reload (even if already loaded)
python scripts/pipeline.py backfill --pipeline my_pipeline --from 2023-04 --to 2023-04 --force

# Fill empty descriptions
python scripts/pipeline.py enrich --pipeline my_pipeline [--table X] [--dry-run]

# Add new sheet
python scripts/pipeline.py add-sheet --pipeline my_pipeline --sheet "Sheet Name"

# View pipelines
python scripts/pipeline.py list

# View load history
python scripts/pipeline.py history --pipeline my_pipeline
```

### Key SQL Queries

```sql
-- All pipelines
SELECT * FROM datawarp.tbl_pipeline_configs;

-- Table metadata (enrichment info)
SELECT * FROM datawarp.v_table_metadata WHERE pipeline_id = 'X';

-- Column metadata (mappings + descriptions)
SELECT * FROM datawarp.v_column_metadata WHERE table_name = 'Y';

-- Load statistics
SELECT * FROM datawarp.v_table_stats;

-- Combined view (metadata + stats)
SELECT * FROM datawarp.v_tables;

-- Enrichment API call logs
SELECT * FROM datawarp.tbl_enrichment_log ORDER BY created_at DESC;

-- Columns needing enrichment
SELECT table_name, original_name
FROM datawarp.v_column_metadata
WHERE column_description = '';

-- Version history
SELECT table_name, mappings_version, last_enriched
FROM datawarp.v_table_metadata;

-- Pattern → Table mapping
SELECT
    sm->>'table_name' as table_name,
    jsonb_array_elements_text(fp->'filename_patterns') as pattern
FROM datawarp.tbl_pipeline_configs c,
     jsonb_array_elements(c.config->'file_patterns') as fp,
     jsonb_array_elements(fp->'sheet_mappings') as sm
WHERE c.pipeline_id = 'your_pipeline_id'
ORDER BY table_name, pattern;

-- File patterns with types
SELECT
    c.pipeline_id,
    jsonb_array_elements_text(fp->'filename_patterns') as pattern,
    fp->'file_types' as file_types,
    jsonb_array_length(fp->'sheet_mappings') as sheet_count
FROM datawarp.tbl_pipeline_configs c,
     jsonb_array_elements(c.config->'file_patterns') as fp
WHERE c.pipeline_id = 'your_pipeline_id';
```

### Metadata Flow Summary

```
Bootstrap                 Scan                    Enrich
─────────                ─────                   ───────
    │                       │                       │
    ▼                       ▼                       ▼
Discover files        Use saved patterns      Get sample data
    │                       │                       │
    ▼                       ▼                       ▼
Detect grain          Detect drift            Call LLM
    │                       │                       │
    ▼                       ▼                       ▼
Call LLM              Add identity mappings   Merge descriptions
    │                 (empty descriptions)          │
    ▼                       │                       ▼
Save config v1        Save config v(n+1)      Save config v(n+1)
    │                       │                       │
    ▼                       ▼                       ▼
Load data             Load data               (no data load)
```

---

## 11. Multi-Pattern Support & Auto-Detection

NHS sometimes changes file naming conventions across years. DataWarp handles this with multi-pattern matching and auto-detection.

### The Problem

A pipeline bootstrapped with 2025 data may have a file pattern like:
```
msds-oct2025-exp-data\.csv
```

But 2023 files might be named differently:
```
msds-apr2023-exp-data-final.csv    (different suffix)
msds-jan2022-experimental-data.csv  (different prefix)
```

The single pattern won't match historical files, breaking backfill.

### Solution: Multiple Filename Patterns

Each `FilePattern` now supports a list of patterns:

```python
@dataclass
class FilePattern:
    filename_patterns: List[str]  # Multiple patterns (was: filename_pattern: str)
    file_types: List[str]
    sheet_mappings: List[SheetMapping]
```

**Backward Compatibility**: Old configs with `filename_pattern` (singular) are auto-migrated to `filename_patterns` (list) when loaded.

### Auto-Detection During Backfill

When backfill finds no files matching existing patterns, it:
1. Downloads unmatched files
2. Compares schema fingerprint (column names)
3. If 70%+ columns match, prompts to add new pattern

**Example Output:**
```
Loading period: 2023-04

  [warning] No match, but found 1 file(s) with compatible schema:
    msds-apr2023-exp-data-final.csv
  Add pattern? [Y/n]: y

  Processing: msds-apr2023-exp-data-final.csv
    tbl_national_maternity_stats: 2304 rows
```

### Backfill Command

```bash
# Load historical periods
python scripts/pipeline.py backfill \
  --pipeline maternity_services_monthly_statistics \
  --from 2023-04 \
  --to 2023-12

# Force reload even if already loaded
python scripts/pipeline.py backfill \
  --pipeline maternity_services_monthly_statistics \
  --from 2023-04 \
  --to 2023-12 \
  --force
```

### Check Pattern → Table Mapping

```sql
-- See which patterns map to which tables
SELECT
    sm->>'table_name' as table_name,
    jsonb_array_elements_text(fp->'filename_patterns') as pattern
FROM datawarp.tbl_pipeline_configs c,
     jsonb_array_elements(c.config->'file_patterns') as fp,
     jsonb_array_elements(fp->'sheet_mappings') as sm
WHERE c.pipeline_id = 'your_pipeline_id'
ORDER BY table_name, pattern;
```

**Example output:**
```
            table_name             |                  pattern
-----------------------------------+-------------------------------------------
 tbl_national_maternity_stats      | msds-[a-z]{3}\d{4}-exp-data\.csv
 tbl_national_maternity_stats      | msds-[a-z]{3}\d{4}-exp-data-final\.csv
 tbl_national_maternity_measures   | msds-[a-z]{3}\d{4}-exp-measures\.csv
```

### Detailed Pattern View

```sql
-- Full pattern details with file types
SELECT
    c.pipeline_id,
    jsonb_array_elements_text(fp->'filename_patterns') as pattern,
    fp->'file_types' as file_types,
    jsonb_array_length(fp->'sheet_mappings') as sheet_count
FROM datawarp.tbl_pipeline_configs c,
     jsonb_array_elements(c.config->'file_patterns') as fp
WHERE c.pipeline_id = 'your_pipeline_id';
```

### Provisional → Final Data Handling

NHS often releases provisional data, then updates with final figures. The `scan` command automatically refreshes the most recent 2 periods:

```
Scanning: Maternity Services Monthly Statistics

  Will load: 2025-01 (new)
  Will refresh: 2024-12, 2024-11 (recent periods)

Loading period: 2024-12
  Replacing 2304 rows
  Processing: msds-dec2024-exp-data.csv
    tbl_national_maternity_stats: 2310 rows (final figures)
```

---

## 12. Data Lineage & Provenance

Understanding where data came from is critical for debugging, auditing, and trust. DataWarp tracks complete lineage from source URL to database table.

### Lineage Data Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA LINEAGE FLOW                                  │
└─────────────────────────────────────────────────────────────────────────────┘

  NHS Landing Page                    Pipeline Config                  Database
  ───────────────                    ───────────────                  ────────
        │                                  │                              │
        ▼                                  ▼                              ▼
┌───────────────────┐            ┌─────────────────────┐        ┌──────────────────┐
│ digital.nhs.uk/   │            │ tbl_pipeline_configs│        │ staging.tbl_*    │
│ .../mi-adhd       │───────────▶│                     │        │                  │
│                   │            │ • pipeline_id       │        │ • actual data    │
│ Source of truth   │            │ • landing_page      │        │ • _period column │
│ for file URLs     │            │ • file_patterns[]   │        │ • _loaded_at     │
└───────────────────┘            │ • sheet_mappings[]  │        └──────────────────┘
        │                        │ • column_mappings   │                 ▲
        │                        └─────────────────────┘                 │
        │                                  │                              │
        ▼                                  ▼                              │
┌───────────────────┐            ┌─────────────────────┐                 │
│ Source Files      │            │ tbl_load_history    │                 │
│                   │            │                     │─────────────────┘
│ adhd_may25.csv    │───────────▶│ • table_name        │
│ adhd_aug25.csv    │            │ • source_file       │
│ data_dict.xlsx    │            │ • sheet_name        │
│                   │            │ • period            │
│ Downloaded &      │            │ • rows_loaded       │
│ processed         │            │ • loaded_at         │
└───────────────────┘            └─────────────────────┘
```

### Query: Full Lineage for a Table

Trace a table back to its source files, landing page, and load history:

```sql
-- Get lineage for a specific table
SELECT
    h.table_name,
    h.pipeline_id,
    c.config->>'name' as publication,
    c.config->>'landing_page' as landing_page,
    h.period,
    h.source_file,
    h.sheet_name,
    h.rows_loaded,
    h.loaded_at
FROM datawarp.tbl_load_history h
LEFT JOIN datawarp.tbl_pipeline_configs c ON h.pipeline_id = c.pipeline_id
WHERE h.table_name = 'tbl_adhd_counts'
ORDER BY h.loaded_at DESC;
```

**Example output:**
```
   table_name    | pipeline_id |  publication  |         landing_page          | period  |   source_file    | sheet_name | rows_loaded |       loaded_at
-----------------+-------------+---------------+-------------------------------+---------+------------------+------------+-------------+------------------------
 tbl_adhd_counts | mi_adhd     | ADHD          | https://digital.nhs.uk/.../   | 2025-11 | adhd_nov25.csv   |            |        8149 | 2025-01-28 14:30:00
 tbl_adhd_counts | mi_adhd     | ADHD          | https://digital.nhs.uk/.../   | 2025-08 | adhd_aug25.csv   |            |        1318 | 2025-01-28 10:15:00
 tbl_adhd_counts | mi_adhd     | ADHD          | https://digital.nhs.uk/.../   | 2025-05 | adhd_may25.csv   |            |        1304 | 2025-01-27 09:00:00
```

### Query: Enrichment Metadata for a Table

Get the semantic enrichment details (column mappings, descriptions, grain):

```sql
-- Get enrichment info from config
SELECT
    pc.pipeline_id,
    pc.config->>'name' as publication,
    pc.config->>'landing_page' as landing_page,
    m->>'table_name' as table_name,
    m->>'table_description' as description,
    m->>'grain' as grain,
    COALESCE((m->>'mappings_version')::int, 1) as version,
    m->>'last_enriched' as last_enriched,
    jsonb_object_keys(m->'column_mappings') as columns
FROM datawarp.tbl_pipeline_configs pc,
     jsonb_array_elements(pc.config->'file_patterns') fp,
     jsonb_array_elements(fp->'sheet_mappings') m
WHERE m->>'table_name' = 'tbl_adhd_counts';
```

**Example output:**
```
 pipeline_id | publication |      landing_page       |   table_name    |        description         | grain | version | last_enriched |   columns
-------------+-------------+-------------------------+-----------------+----------------------------+-------+---------+---------------+-------------
 mi_adhd     | ADHD        | https://digital.nhs.uk/ | tbl_adhd_counts | ADHD referrals by category | icb   |       3 | 2025-01-28    | indicator
 mi_adhd     | ADHD        | https://digital.nhs.uk/ | tbl_adhd_counts | ADHD referrals by category | icb   |       3 | 2025-01-28    | period_start
 mi_adhd     | ADHD        | https://digital.nhs.uk/ | tbl_adhd_counts | ADHD referrals by category | icb   |       3 | 2025-01-28    | period_end
 mi_adhd     | ADHD        | https://digital.nhs.uk/ | tbl_adhd_counts | ADHD referrals by category | icb   |       3 | 2025-01-28    | count
```

### Quick Lineage Using Views

For common queries, use the pre-built views:

```sql
-- Table-level metadata (one row per table)
SELECT * FROM datawarp.v_table_metadata
WHERE table_name = 'tbl_adhd_counts';

-- Column-level metadata (one row per column)
SELECT * FROM datawarp.v_column_metadata
WHERE table_name = 'tbl_adhd_counts';

-- Combined metadata + load stats
SELECT * FROM datawarp.v_tables
WHERE table_name = 'tbl_adhd_counts';
```

### Lineage Diagram: Single Table

```
                              tbl_adhd_counts
                             ┌───────────────┐
                             │               │
                             │  8,771 rows   │
                             │  3 periods    │
                             │               │
                             └───────┬───────┘
                                     │
           ┌─────────────────────────┼─────────────────────────┐
           │                         │                         │
           ▼                         ▼                         ▼
    ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
    │ 2025-05     │          │ 2025-08     │          │ 2025-11     │
    │ 1,304 rows  │          │ 1,318 rows  │          │ 8,149 rows  │
    │             │          │             │          │             │
    └──────┬──────┘          └──────┬──────┘          └──────┬──────┘
           │                        │                        │
           ▼                        ▼                        ▼
    adhd_may25.csv           adhd_aug25.csv           adhd_nov25.csv
           │                        │                        │
           └────────────────────────┼────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │  NHS Digital Landing Page     │
                    │  digital.nhs.uk/.../mi-adhd   │
                    └───────────────────────────────┘
```

### Answering Lineage Questions

| Question | Query |
|----------|-------|
| Where did this table come from? | `v_table_metadata` → `landing_page` |
| What file was loaded for period X? | `tbl_load_history` → `source_file` |
| When was this data last refreshed? | `tbl_load_history` → `MAX(loaded_at)` |
| How many rows per period? | `tbl_load_history` → `rows_loaded` |
| What columns were enriched? | `v_column_metadata` → `is_enriched = true` |
| What's the data grain? | `v_table_metadata` → `grain` |

---

## 13. MCP Server: Exposing Data to Claude

The Model Context Protocol (MCP) lets Claude access DataWarp's NHS data through a standardized interface. This section explains the architecture, available tools, and how to configure Claude Desktop.

### What is MCP?

MCP is a protocol that allows AI assistants like Claude to:
- Discover available datasets
- Understand column meanings (via enriched metadata)
- Execute SQL queries
- Trace data lineage

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MCP ARCHITECTURE                                     │
└─────────────────────────────────────────────────────────────────────────────┘

   User                     Claude Desktop              DataWarp MCP Server
   ────                     ──────────────              ───────────────────
     │                            │                              │
     │  "What ADHD data          │                              │
     │   do you have?"           │                              │
     │ ─────────────────────────▶│                              │
     │                            │   list_datasets()            │
     │                            │ ────────────────────────────▶│
     │                            │                              │
     │                            │   [{name: tbl_adhd_counts,   │
     │                            │     description: "ADHD...",  │
     │                            │     grain: "icb", ...}]      │
     │                            │◀────────────────────────────│
     │                            │                              │
     │  "Here are 3 tables       │                              │
     │   with ADHD data..."      │                              │
     │◀─────────────────────────│                              │
     │                            │                              │
     │  "Show me referrals       │                              │
     │   by ICB for Nov 2025"    │                              │
     │ ─────────────────────────▶│                              │
     │                            │   get_schema('tbl_adhd...')  │
     │                            │ ────────────────────────────▶│
     │                            │                              │
     │                            │   {columns: [...],           │
     │                            │    descriptions: {...}}      │
     │                            │◀────────────────────────────│
     │                            │                              │
     │                            │   query("SELECT icb_code...") │
     │                            │ ────────────────────────────▶│
     │                            │                              │
     │                            │   {rows: [...]}              │
     │                            │◀────────────────────────────│
     │                            │                              │
     │  [formatted table         │                              │
     │   with results]           │                              │
     │◀─────────────────────────│                              │
```

### MCP Tools

DataWarp exposes 5 tools through MCP:

| Tool | Purpose | Example Use |
|------|---------|-------------|
| `list_datasets` | Discover available tables | "What data do you have?" |
| `get_schema` | Get column names & descriptions | "What columns are in this table?" |
| `query` | Execute SQL (SELECT only) | "Show me top 10 ICBs by referrals" |
| `get_periods` | List available time periods | "What months of data exist?" |
| `get_lineage` | Trace data provenance | "Where did this data come from?" |

### Tool Details

#### 1. list_datasets

Returns all tables with enriched metadata.

**Response includes:**
```json
{
  "name": "tbl_adhd_counts",
  "description": "ADHD referrals by category",
  "grain": "icb",
  "grain_description": "Integrated Care Board level data",
  "row_count": 8149,
  "periods": ["2025-05", "2025-08", "2025-11"],
  "pipeline_id": "mi_adhd",
  "publication_name": "ADHD",
  "landing_page": "https://digital.nhs.uk/.../mi-adhd",
  "has_enriched_columns": true,
  "mappings_version": 3
}
```

#### 2. get_schema

Returns column-level metadata including original names, semantic names, and descriptions.

**Response includes:**
```json
{
  "table_name": "tbl_adhd_counts",
  "description": "ADHD referrals by category",
  "grain": "icb",
  "columns": [
    {
      "name": "icb_code",
      "original_name": "org_code",
      "type": "TEXT",
      "description": "Integrated Care Board organisation code",
      "is_enriched": true
    }
  ]
}
```

#### 3. query

Execute SQL queries (SELECT only, auto-limited to 1000 rows).

**Safety features:**
- Only SELECT statements allowed
- Auto-adds LIMIT if not present
- Returns structured results

#### 4. get_periods

Returns available time periods for a table.

```json
["2025-05", "2025-08", "2025-11"]
```

#### 5. get_lineage

Returns complete provenance: source, load history, enrichment status.

```json
{
  "table_name": "tbl_adhd_counts",
  "source": {
    "pipeline_id": "mi_adhd",
    "publication": "ADHD",
    "landing_page": "https://digital.nhs.uk/...",
    "sheet_name": null,
    "file_pattern": "adhd_[a-z]{3}\\d{2}\\.csv"
  },
  "loads": [
    {"period": "2025-11", "file": "adhd_nov25.csv", "rows": 8149, "loaded_at": "..."},
    {"period": "2025-08", "file": "adhd_aug25.csv", "rows": 1318, "loaded_at": "..."}
  ],
  "enrichment": {
    "version": 3,
    "last_enriched": "2025-01-28",
    "columns_total": 15,
    "columns_enriched": 12,
    "columns_pending": 3
  }
}
```

### Data Flow: Config → MCP

All metadata comes from the JSONB config (single source of truth):

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SINGLE SOURCE OF TRUTH                                    │
└─────────────────────────────────────────────────────────────────────────────┘

                      tbl_pipeline_configs.config (JSONB)
                     ┌─────────────────────────────────────┐
                     │                                     │
                     │  file_patterns: [{                  │
                     │    filename_patterns: [...],        │
                     │    sheet_mappings: [{               │
                     │      table_name,        ─────────────────▶ list_datasets
                     │      table_description, ─────────────────▶ get_schema
                     │      column_mappings,   ─────────────────▶ get_schema
                     │      column_descriptions,────────────────▶ get_schema
                     │      grain,             ─────────────────▶ list_datasets
                     │      mappings_version   ─────────────────▶ get_lineage
                     │    }]                               │
                     │  }]                                 │
                     │                                     │
                     └─────────────────────────────────────┘
                                      │
                                      │ No separate metadata tables
                                      │ No drift possible
                                      ▼
                     ┌─────────────────────────────────────┐
                     │         MCP Server reads            │
                     │         directly from config        │
                     └─────────────────────────────────────┘
```

### Configuring Claude Desktop

To connect Claude Desktop to DataWarp's MCP server:

#### Step 1: Locate Config File

```bash
# macOS
~/Library/Application Support/Claude/claude_desktop_config.json

# Windows
%APPDATA%\Claude\claude_desktop_config.json

# Linux
~/.config/Claude/claude_desktop_config.json
```

#### Step 2: Add MCP Server Configuration

Edit `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "datawarp-nhs": {
      "command": "python",
      "args": [
        "/path/to/datawarp-v3.1/scripts/mcp_server.py"
      ],
      "env": {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_DB": "datawalker",
        "POSTGRES_USER": "databot",
        "POSTGRES_PASSWORD": "databot",
        "PYTHONPATH": "/path/to/datawarp-v3.1/src"
      }
    }
  }
}
```

#### Step 3: Restart Claude Desktop

Close and reopen Claude Desktop. You should see "datawarp-nhs" in the MCP tools menu.

### Testing MCP Server

Before configuring Claude Desktop, test the server locally:

```bash
# Test mode - shows what MCP would return
PYTHONPATH=src python scripts/mcp_server.py --test
```

**Expected output:**
```
DataWarp MCP Server - Test Mode

1. list_datasets()
┌────────────────────┬─────────────────────────────┬───────┬──────────┬─────────┐
│ Table              │ Description                 │ Rows  │ Enriched │ Version │
├────────────────────┼─────────────────────────────┼───────┼──────────┼─────────┤
│ tbl_adhd_counts    │ ADHD referrals by category  │ 8149  │ Yes      │ 3       │
│ tbl_national_adhd  │ National ADHD metrics       │ 126   │ Yes      │ 1       │
└────────────────────┴─────────────────────────────┴───────┴──────────┴─────────┘

2. get_schema('tbl_adhd_counts')
  Pipeline: mi_adhd
  Version: 3
  Last enriched: 2025-01-28
  Column mappings: 15 total, 12 enriched

3. get_lineage('tbl_adhd_counts')
  Pipeline: mi_adhd
  Publication: ADHD
  Enrichment: v3, 12/15 enriched, 3 pending
┌─────────┬──────────────────┬───────┬─────────────────────┐
│ Period  │ File             │ Rows  │ Loaded At           │
├─────────┼──────────────────┼───────┼─────────────────────┤
│ 2025-11 │ adhd_nov25.csv   │ 8149  │ 2025-01-28 14:30:00 │
│ 2025-08 │ adhd_aug25.csv   │ 1318  │ 2025-01-28 10:15:00 │
└─────────┴──────────────────┴───────┴─────────────────────┘

MCP server is ready!
```

### Example Claude Conversation

Once configured, Claude can query your data:

**User:** "What NHS data do you have?"

**Claude:** (calls `list_datasets`)
> I have access to 3 NHS datasets:
> 1. **tbl_adhd_counts** - ADHD referrals by category (ICB level, 8,149 rows)
> 2. **tbl_national_adhd_metrics** - National ADHD metrics definitions (126 rows)
> 3. **tbl_adhd_indicators** - Historical ADHD indicator data (1,304 rows)

**User:** "Show me the top 5 ICBs by ADHD referrals in November 2025"

**Claude:** (calls `get_schema`, then `query`)
```sql
SELECT icb_name, SUM(referrals_received) as total_referrals
FROM staging.tbl_adhd_counts
WHERE period = '2025-11'
GROUP BY icb_name
ORDER BY total_referrals DESC
LIMIT 5
```

> | ICB | Total Referrals |
> |-----|-----------------|
> | NHS Greater Manchester | 4,521 |
> | NHS West Yorkshire | 3,892 |
> | ... | ... |

**User:** "Where does this data come from?"

**Claude:** (calls `get_lineage`)
> This data comes from the **ADHD** publication on NHS Digital:
> - Landing page: https://digital.nhs.uk/.../mi-adhd
> - Latest file: adhd_nov25.csv (loaded 2025-01-28)
> - Enrichment: version 3, 12 of 15 columns have semantic names

### Troubleshooting MCP

| Problem | Solution |
|---------|----------|
| Claude doesn't see MCP tools | Restart Claude Desktop, check config path |
| "Connection refused" | Ensure PostgreSQL is running |
| Empty descriptions | Run `enrich --pipeline X` to fill descriptions |
| Stale data | Run `scan --pipeline X` to load new periods |

---

## 14. CLI Commands Reference

Complete reference for all DataWarp CLI commands with detailed examples and nuances.

### Command Overview

| Command | Purpose | LLM Calls? | Modifies Config? |
|---------|---------|------------|------------------|
| `bootstrap` | Initial setup from NHS URL | Yes (if `--enrich`) | Creates new |
| `scan` | Load newly published periods | No | Updates `loaded_periods` |
| `backfill` | Load historical periods | No | Updates `loaded_periods` |
| `reset` | Clear data, keep enrichment | No | Clears `loaded_periods` |
| `enrich` | Fill empty column descriptions | Yes | Updates mappings |
| `add-sheet` | Add new sheet to pipeline | Yes (if `--enrich`) | Adds sheet mapping |
| `list` | Show all pipelines | No | No |
| `history` | Show load history | No | No |

---

### bootstrap

**Purpose:** Create a new pipeline from an NHS URL. Discovers files, optionally enriches with LLM, loads data, saves config.

```bash
python scripts/pipeline.py bootstrap --url <URL> --id <ID> [--enrich] [--name <NAME>]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--url` | NHS landing page URL (required) |
| `--id` | Pipeline ID (auto-generated from URL if omitted) |
| `--name` | Human-readable name (auto-generated if omitted) |
| `--enrich` | Call LLM for semantic column names (recommended) |
| `--skip-unknown` | Skip sheets with no detected entity (default: true) |

**What it does:**
1. Classifies URL (discovery mode, frequency)
2. Scrapes landing page for files
3. Groups files by period
4. Prompts user to select period and files
5. For each file: detects grain, enriches (if `--enrich`), loads to staging
6. Saves pipeline config as JSONB

**Re-bootstrapping existing pipelines:**
```
╭───────── Existing Pipeline ─────────╮
│ Pipeline already exists!            │
│ Tables: tbl_icb_sessions            │
╰─────────────────────────────────────╯

Re-bootstrap anyway? [y/N]: y

Drop existing tables before re-bootstrap? [Y/n]: y
  Dropped staging.tbl_icb_sessions
```

**Nuances:**
- Re-bootstrap offers to drop old tables to avoid duplicates (LLM may generate different names)
- Two-stage enrichment: extracts context from Notes/Contents sheets first, uses it for better naming
- Config stores `file_context` for MCP access to KPI definitions

---

### scan

**Purpose:** Find and load newly published periods using saved patterns. No LLM calls - uses saved column mappings.

```bash
python scripts/pipeline.py scan --pipeline <ID> [--dry-run] [--force-scrape]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--pipeline` | Pipeline ID (required) |
| `--dry-run` | Show what would be loaded, don't load |
| `--force-scrape` | Force landing page scrape even in template mode |

**How scan determines what to load:**

```python
# 1. Get periods available on website
available = [2024-01, 2024-02, 2024-03, 2024-04, 2024-05, 2024-06]

# 2. Compare against config.loaded_periods
loaded = [2024-01, 2024-02, 2024-03]
new_periods = available - loaded  # [2024-04, 2024-05, 2024-06]

# 3. Also refresh 2 most recent (provisional → final)
recent = [2024-06, 2024-05]
to_load = new_periods + recent  # [2024-04, 2024-05, 2024-06]
```

**Example output:**
```
Scanning: ADHD
Discovery mode: template

  o august-2025: 3 files
  o november-2025: 5 files

New period(s): 2025-08, 2025-11
Refreshing: 2025-05 (provisional → final)

Loading period: 2025-08
  Processing: adhd_aug25.csv
    tbl_adhd_counts: 1318 rows
```

**After reset:** When `loaded_periods = []`, scan loads ALL available periods:
```bash
# Reset clears loaded_periods
python scripts/pipeline.py reset --pipeline mi_adhd

# Scan now sees everything as "new"
python scripts/pipeline.py scan --pipeline mi_adhd
# Loads: 2024-01, 2024-02, 2024-03, 2024-04, 2024-05, 2024-06 (all)
```

**Nuances:**
- Always refreshes 2 most recent periods (NHS publishes provisional, then final)
- Uses saved `column_mappings` from config - no LLM calls
- Detects column drift and adds identity mappings for new columns
- Updates `config.loaded_periods` after successful load

---

### backfill

**Purpose:** Load historical periods. Useful for loading data older than what's on the landing page.

```bash
python scripts/pipeline.py backfill --pipeline <ID> --from <YYYY-MM> --to <YYYY-MM> [--force]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--pipeline` | Pipeline ID (required) |
| `--from` | Start period (required, format: YYYY-MM) |
| `--to` | End period (required, format: YYYY-MM) |
| `--force` | Load even if period already in `loaded_periods` |

**Use cases:**
```bash
# Load specific historical range
python scripts/pipeline.py backfill --pipeline mi_adhd --from 2023-01 --to 2023-12

# Force reload a period (e.g., data was corrupted)
python scripts/pipeline.py backfill --pipeline mi_adhd --from 2024-06 --to 2024-06 --force

# After reset, load specific periods (not all)
python scripts/pipeline.py reset --pipeline mi_adhd --yes
python scripts/pipeline.py backfill --pipeline mi_adhd --from 2024-01 --to 2024-03
```

**Auto-pattern detection:**

When backfill finds files that don't match existing patterns:
```
Loading period: 2023-04

  [warning] No match, but found 1 file(s) with compatible schema:
    msds-apr2023-exp-data-final.csv
  Add pattern? [Y/n]: y

  Processing: msds-apr2023-exp-data-final.csv
    tbl_national_maternity_stats: 2304 rows
```

**Nuances:**
- Skips periods already in `loaded_periods` unless `--force`
- Generates period URLs from template pattern
- Auto-detects schema-compatible files with different naming
- Adds new filename patterns to config when discovered

---

### reset

**Purpose:** Clear loaded data while preserving expensive LLM-generated enrichment (table names, column mappings, descriptions).

```bash
python scripts/pipeline.py reset --pipeline <ID> [--yes]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--pipeline` | Pipeline ID (required) |
| `--yes`, `-y` | Skip confirmation prompt |

**What reset does:**
1. Drops staging tables for the pipeline
2. Clears `tbl_load_history` for the pipeline
3. Sets `config.loaded_periods = []`

**What reset preserves:**
- `table_name` (LLM-generated)
- `table_description` (LLM-generated)
- `column_mappings` (LLM-generated)
- `column_descriptions` (LLM-generated)
- `file_patterns`, `sheet_mappings`
- `file_context` (extracted KPIs, methodology)

**Example output:**
```
╭─────────── Reset Pipeline Data ───────────╮
│ Pipeline: iucadc_new_from_april_2021      │
│ Name: IUCADC New From April 2021          │
│ Periods loaded: 3                         │
│ Tables: 2                                 │
╰───────────────────────────────────────────╯

      Tables to clear
┌─────────────────────────┬───────────┐
│ Table                   │ Status    │
├─────────────────────────┼───────────┤
│ staging.tbl_icb_sessions│ 4521 rows │
│ staging.tbl_icb_triage  │ 1203 rows │
└─────────────────────────┴───────────┘

This will:
  1. Drop staging tables listed above
  2. Clear load history for this pipeline
  3. Reset loaded_periods to empty

Preserved: table names, column mappings, descriptions (enrichment)

Proceed with reset? [y/N]: y
  Dropped staging.tbl_icb_sessions
  Dropped staging.tbl_icb_triage
  Cleared load history
  Reset loaded_periods

Reset complete!

To reload data: python scripts/pipeline.py scan --pipeline iucadc_new_from_april_2021
```

**Typical workflow:**
```bash
# 1. Reset (clears data, keeps enrichment)
python scripts/pipeline.py reset --pipeline mi_adhd --yes

# 2. Reload all periods (uses saved mappings, no LLM cost)
python scripts/pipeline.py scan --pipeline mi_adhd

# Or reload specific periods
python scripts/pipeline.py backfill --pipeline mi_adhd --from 2024-01 --to 2024-06
```

**When to use reset:**
- Data corruption and need to reload from source
- Testing changes to loader code
- Schema changed and need fresh tables
- Disk space cleanup (can reload later)

---

### enrich

**Purpose:** Fill empty column descriptions using LLM. Run after drift detection adds new columns.

```bash
python scripts/pipeline.py enrich --pipeline <ID> [--table <NAME>] [--dry-run] [--force]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--pipeline` | Pipeline ID (required) |
| `--table` | Specific table to enrich (optional) |
| `--dry-run` | Show what would be enriched, don't call LLM |
| `--force` | Re-enrich all columns, even those with descriptions |

**Example:**
```bash
# Check what needs enrichment
python scripts/pipeline.py enrich --pipeline mi_adhd --dry-run

# Enrich empty descriptions
python scripts/pipeline.py enrich --pipeline mi_adhd

# Force re-enrich everything (regenerate all descriptions)
python scripts/pipeline.py enrich --pipeline mi_adhd --force
```

**Nuances:**
- Only enriches columns where `description = ''`
- Bumps `mappings_version` after enrichment
- Logs LLM calls to `tbl_enrichment_log`

---

### add-sheet

**Purpose:** Add a new sheet from an existing file to the pipeline.

```bash
python scripts/pipeline.py add-sheet --pipeline <ID> --sheet <NAME> [--no-enrich]
```

**Options:**
| Option | Description |
|--------|-------------|
| `--pipeline` | Pipeline ID (required) |
| `--sheet` | Sheet name to add (required) |
| `--no-enrich` | Skip LLM enrichment |

---

### list

**Purpose:** Show all registered pipelines.

```bash
python scripts/pipeline.py list
```

**Output:**
```
╭─────────────────────────── Registered Pipelines ───────────────────────────╮
│                                                                             │
│  mi_adhd                                                                    │
│    Name: ADHD                                                               │
│    URL: https://digital.nhs.uk/.../mi-adhd                                  │
│    Periods: 3 loaded                                                        │
│    Tables: tbl_adhd_counts, tbl_national_adhd_metrics                       │
│                                                                             │
│  maternity_services                                                         │
│    Name: Maternity Services Monthly Statistics                              │
│    URL: https://digital.nhs.uk/.../maternity-services                       │
│    Periods: 12 loaded                                                       │
│    Tables: tbl_national_maternity_stats                                     │
│                                                                             │
╰─────────────────────────────────────────────────────────────────────────────╯
```

---

### history

**Purpose:** Show load history for a pipeline.

```bash
python scripts/pipeline.py history --pipeline <ID>
```

**Output:**
```
╭────────────────────── Load History: mi_adhd ──────────────────────╮
│                                                                    │
│  Period   │ Table              │ Rows  │ File           │ Loaded  │
│  ─────────┼────────────────────┼───────┼────────────────┼──────── │
│  2025-11  │ tbl_adhd_counts    │ 8149  │ adhd_nov25.csv │ Jan 28  │
│  2025-08  │ tbl_adhd_counts    │ 1318  │ adhd_aug25.csv │ Jan 28  │
│  2025-05  │ tbl_adhd_counts    │ 1304  │ adhd_may25.csv │ Jan 27  │
│                                                                    │
╰────────────────────────────────────────────────────────────────────╯
```

---

### Command Workflow Diagram

```
                                    ┌─────────────┐
                                    │   START     │
                                    └──────┬──────┘
                                           │
                                           ▼
                              ┌────────────────────────┐
                              │  bootstrap --enrich    │
                              │  (creates pipeline,    │
                              │   LLM enrichment)      │
                              └────────────┬───────────┘
                                           │
                                           ▼
                    ┌──────────────────────────────────────────┐
                    │                                          │
                    ▼                                          ▼
          ┌─────────────────┐                        ┌─────────────────┐
          │  scan           │                        │  backfill       │
          │  (new periods)  │                        │  (historical)   │
          └────────┬────────┘                        └────────┬────────┘
                   │                                          │
                   │     ┌────────────────────┐               │
                   │     │  Column drift?     │               │
                   └────▶│  New columns added │◀──────────────┘
                         └─────────┬──────────┘
                                   │ yes
                                   ▼
                         ┌─────────────────┐
                         │  enrich         │
                         │  (fill empty    │
                         │   descriptions) │
                         └─────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │  Need to reload data?    │
                    │  (corruption, testing)   │
                    └────────────┬─────────────┘
                                 │ yes
                                 ▼
                    ┌──────────────────────────┐
                    │  reset                   │
                    │  (clears data, keeps     │
                    │   enrichment)            │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │  scan or backfill        │
                    │  (reload with saved      │
                    │   mappings, no LLM cost) │
                    └──────────────────────────┘
```

---

## Troubleshooting

### "No columns needed enrichment"

The LLM returned semantic names that don't match your column names. Check:
```sql
SELECT original_name, semantic_name
FROM datawarp.v_column_metadata
WHERE table_name = 'your_table';
```

### Enrichment fails

Check the enrichment log:
```sql
SELECT sheet_name, error_message, created_at
FROM datawarp.tbl_enrichment_log
WHERE success = false
ORDER BY created_at DESC;
```

### Column mappings not applied

Verify the loader received the mappings:
```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'staging' AND table_name = 'your_table';
```

Compare against:
```sql
SELECT semantic_name
FROM datawarp.v_column_metadata
WHERE table_name = 'your_table';
```

---

## 15. Database Schema

DataWarp uses PostgreSQL with 2 schemas:
- `datawarp` - Configuration and metadata (4 tables, 5 views)
- `staging` - Loaded NHS data (dynamic tables)

### Core Tables

| Table | Purpose |
|-------|---------|
| `tbl_pipeline_configs` | Pipeline config as JSONB (patterns, mappings, descriptions) |
| `tbl_load_history` | What was loaded (period, table, rows, source metrics) |
| `tbl_enrichment_log` | LLM API calls (tokens, cost, suggestions) |
| `tbl_cli_runs` | CLI command tracking (timing, status) |

### Views

| View | Purpose |
|------|---------|
| `v_table_metadata` | Table names, descriptions, grain from config |
| `v_column_metadata` | Column mappings and descriptions |
| `v_table_stats` | Row counts, periods loaded per table |
| `v_tables` | Combined metadata + stats |
| `v_load_reconciliation` | Source rows vs loaded rows comparison |

**Source of truth:** `sql/schema.sql`

---

## 16. SQL Verification Queries

### Check data loaded correctly
```sql
-- Total rows per table
SELECT table_name, SUM(rows_loaded) as total_rows
FROM datawarp.tbl_load_history
WHERE pipeline_id = 'your_pipeline'
GROUP BY table_name;

-- Verify no data loss (reconciliation)
SELECT table_name, source_rows, rows_loaded, reconciliation_status
FROM datawarp.v_load_reconciliation
WHERE pipeline_id = 'your_pipeline'
  AND reconciliation_status != 'match';
-- Expected: 0 rows
```

### Check enrichment applied
```sql
-- Columns with semantic names
SELECT table_name, original_name, semantic_name, is_enriched
FROM datawarp.v_column_metadata
WHERE pipeline_id = 'your_pipeline'
  AND is_enriched = true;

-- Enrichment API costs
SELECT SUM(cost_usd) as total_cost, SUM(total_tokens) as tokens
FROM datawarp.tbl_enrichment_log
WHERE pipeline_id = 'your_pipeline';
```

### Check staging tables
```sql
-- List all staging tables
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'staging';

-- Row count with periods
SELECT COUNT(*) as rows, COUNT(DISTINCT period) as periods
FROM staging.your_table;
```

---

## 17. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                   │
│                                                                         │
│   NHS URL → Discovery → Grain Detection → LLM Enrichment → Load → MCP  │
│                                                                         │
│   scraper.py   grain.py      enrich.py        excel.py    mcp_server.py│
└─────────────────────────────────────────────────────────────────────────┘
```

### Component Summary

| Layer | Location | Purpose |
|-------|----------|---------|
| Discovery | `src/datawarp/discovery/` | Scrape NHS pages, find files, detect periods |
| Metadata | `src/datawarp/metadata/` | Detect grain (ICB/Trust/GP), LLM enrichment |
| Loader | `src/datawarp/loader/` | Extract Excel/CSV, load to PostgreSQL |
| Pipeline | `src/datawarp/pipeline/` | Config storage, load history, orchestration |
| CLI | `src/datawarp/cli/` | Bootstrap, scan, backfill, reset commands |
| MCP | `scripts/mcp_server.py` | Expose metadata and query capability to Claude |

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| JSONB config | Single column stores entire PipelineConfig - no schema sprawl |
| DataFrame is truth | DDL and COPY both use `df.columns` - prevents column drift |
| Grain before enrich | Skip useless sheets (notes, methodology) early |
| Append-only loading | `period` column tracks data across time |
| Enrichment logging | Full observability of LLM costs and suggestions |

---

## Archived Documentation

Historical design docs and specs are preserved in `docs/archive/`.
