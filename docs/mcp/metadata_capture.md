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
