# DataWarp Architecture Analysis & Rescue Plan

## The Real Problem

It is not the DDL bug. The DDL bug is a symptom.

The real problem is that **NHS publication URLs are archives, not datasets**. When you scrape MSA, you get 95 files spanning years of monthly snapshots plus supporting documentation. Showing users 95 files is overwhelming. What users actually want is:

> "Give me December 2024's data"

That is 4-6 files, not 95.

And critically, you need the system to **remember what it learned** so that when January 2025's data appears, it loads automatically into the same tables.

## The Solution: Pipeline Lifecycle

```
┌─────────────────────────────────────────────────────────────────┐
│                      BOOTSTRAP (one-time)                       │
│  User discovers → selects files/sheets → loads → system learns  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PIPELINE CONFIG (stored)                      │
│  • File patterns: "MSA-Data-*.xlsx"                             │
│  • Sheet mappings: "ICB Level" → tbl_msa_icb                    │
│  • Column mappings: icb_code → icb_code, incomplete → incomplete │
│  • Loaded periods: [2024-11, 2024-12]                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     AUTO-LOAD (recurring)                        │
│  Scan finds new period → matches patterns → loads to same tables │
└─────────────────────────────────────────────────────────────────┘
```

Bootstrap PRODUCES the configuration that Auto-load CONSUMES. This is the bridge your existing code was missing.

## Period-First Grouping

Instead of:
```
Discovered 95 files:
  1. MSA-Data-Nov-2024.xlsx
  2. MSA-Data-Oct-2024.xlsx
  3. MSA-Data-Sep-2024.xlsx
  ... 92 more files
```

Present:
```
Available Periods:
  2024-12  5 data files  ← LATEST
  2024-11  5 data files
  2024-10  5 data files
  ... +36 more periods

Default: 2024-12 (latest)
```

The discrete unit users care about is a **monthly snapshot**, not individual files.

## File Classification

Within each period, files are classified:

| Category | Examples | Action |
|----------|----------|--------|
| **data** | `MSA-Data.xlsx`, `ICB-Level.csv` | Load to database |
| **supporting** | `Summary.pdf`, `Dashboard.xlsx` | Show but deprioritise |
| **methodology** | `Technical-Guide.pdf`, `Data-Dictionary.xlsx` | Hide by default |

Primary data files are identified by patterns like `by-icb`, `by-trust`, `national`, `england` in the filename. These are auto-selected in `--auto` mode.

---

## Secondary Problem: The DDL Bug

You have **27,000 lines** of well-designed Python across excellent individual components that don't compose cleanly. The integration pain is not in any single module but in the **handoff points** between them.

## Column Naming: The DDL Bug Root Cause

The bug stems from column names being generated at multiple points with different naming strategies:

```
FileExtractor          Models Layer           Loader
─────────────          ────────────           ──────
original_headers   →   ColumnAlias.alias  →   ColumnMatch.header
       ↓                    ↓                      ↓
   pg_name         →   canonical_name     →   canonical_name
       ↓                                           ↓
 final_name        ────────────────────────→   INSERT columns
       ↓
extract_data() keys
```

**The mismatch**: 
- `extract_data()` produces dicts with keys = `final_name` (which is `pg_name` unless `semantic_name` is set)
- DDL is created from `ModelColumn.canonical_name`
- `ColumnMatch.header` must exactly match `final_name` for the mapping to work

If the header string in your aliases doesn't match what `FileExtractor` produces, the INSERT will try to use columns that don't exist in the DDL.

## The Unified Flow Fix

The `unified_flow.py` script fixes this by:

1. **Single source of truth for column names**: Uses pandas to read data, sanitises columns ONCE, then uses those exact names for both DDL and INSERT

2. **No intermediate mapping**: Skips the Models layer entirely for the MVP, going straight from file to table

3. **Consistent column handling**:
```python
# Step 1: Read with pandas
df = pd.read_excel(file_path, sheet_name=sheet_name)

# Step 2: Sanitise columns ONCE
for orig_col in df.columns:
    sanitized = sanitize_column_name(str(orig_col))
    col_mapping[orig_col] = sanitized

# Step 3: Apply to DataFrame (now df.columns = sanitized names)
df.columns = [col_mapping[c] for c in df.columns]

# Step 4: Create DDL using df.columns
for col in df.columns:
    pg_type = infer_pg_type(df[col])
    col_defs.append(f'"{col}" {pg_type}')

# Step 5: INSERT using same df.columns
cur.copy_expert(
    f"COPY table ({', '.join(df.columns)}) FROM STDIN",
    buffer
)
```

The key insight: **pandas DataFrame.columns is the single source of truth**. DDL and INSERT both reference it directly.

## Architectural Layers (What to Keep vs. Simplify)

### Keep (Working Well)
- **Khoj scraper** (`khoj/scraper.py`): Excellent URL discovery
- **Period parsing** (`utils/period.py`): Robust date extraction
- **FileExtractor structure analysis** (`core/extractor.py`): Good header detection

### Simplify (Over-engineered for MVP)
- **Agent state machine** (`agent/state_machine.py`): Replace with simple CLI flow
- **Queue system** (`queue/manager.py`): Overkill for single-user loading
- **39 database tables**: Reduce to ~5 for MVP

### Fix (Broken Integration)
- **Column mapping chain**: Consolidate to single sanitisation point
- **Model bootstrapping**: Make optional, not required for loading

## Recommended MVP Database Schema

```sql
-- Just 5 tables for MVP
CREATE TABLE staging._metadata (
    table_name VARCHAR(63),
    column_name VARCHAR(63),
    pg_type VARCHAR(50),
    sample_values TEXT,
    source_url TEXT,
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (table_name, column_name)
);

CREATE TABLE staging._load_history (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(63),
    source_file TEXT,
    sheet_name VARCHAR(100),
    period VARCHAR(20),
    rows_loaded INT,
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- Plus your actual data tables in staging schema
-- CREATE TABLE staging.tbl_adhd_icb (...)
```

## Usage Patterns

### Quick Single File Load
```bash
python scripts/unified_flow.py \
    --file ./downloads/adhd_2024.xlsx \
    --sheet "ICB Level" \
    --table adhd_icb_2024
```

### Interactive Discovery + Load
```bash
python scripts/unified_flow.py \
    --url "https://digital.nhs.uk/.../mi-adhd"

# Outputs:
# 1. List of discovered files
# 2. Prompt to select files
# 3. List of sheets per file
# 4. Prompt to select sheets
# 5. Load each to PostgreSQL
```

### Batch Load All Files
```bash
python scripts/unified_flow.py \
    --url "https://digital.nhs.uk/.../mi-adhd" \
    --batch \
    --file-types xlsx \
    --prefix adhd
```

## Migration Path

1. **Week 1**: Use `unified_flow.py` to load actual data
2. **Week 2**: Add metadata enrichment (can reuse your LLM column descriptions)
3. **Week 3**: Build MCP server against staging tables
4. **Week 4**: Gradually reintegrate working components from existing codebase

## Files Created

| File | Purpose |
|------|---------|
| `scripts/pipeline.py` | **Primary**: Full lifecycle management - bootstrap, scan, backfill |
| `scripts/smart_flow.py` | Period-aware discovery for ad-hoc loading |
| `scripts/unified_flow.py` | Basic end-to-end flow without persistence |
| `scripts/diagnose_ddl.py` | Traces column naming to identify mismatches |

## Pipeline Commands

### Bootstrap (one-time setup)
```bash
python scripts/pipeline.py bootstrap \
    --url "https://digital.nhs.uk/.../mi-adhd"

# Interactive flow:
# 1. Discovers 95 files, groups into 38 periods
# 2. Asks which period to use for learning (defaults to latest)
# 3. Shows files in that period, asks which to include
# 4. For each file, shows sheets, asks which to load
# 5. Loads data, learns column mappings
# 6. Saves pipeline config for future auto-loads
```

### Scan for new data (recurring)
```bash
# Check for new periods and load if found
python scripts/pipeline.py scan --pipeline adhd

# Dry run - see what would be loaded
python scripts/pipeline.py scan --pipeline adhd --dry-run
```

### Backfill historical data
```bash
# Load all available history
python scripts/pipeline.py backfill --pipeline adhd

# Load from specific period onwards
python scripts/pipeline.py backfill --pipeline adhd --from 2023-01
```

### List registered pipelines
```bash
python scripts/pipeline.py list

# Output:
# ID      Name              Patterns  Periods  Auto-load
# adhd    ADHD Referrals    2         14       ✓
# msa     MSA Data          3         38       ✓
```

## Key Insight

Your existing codebase has all the pieces. The problem is not the individual components but the **coupling between them**. The unified flow decouples by:

1. Making the Models layer optional (not on critical path)
2. Using pandas as the column name authority
3. Eliminating the queue for single-user scenarios
4. Providing one CLI instead of three

Once data actually loads, you can layer back the sophistication incrementally.

## Relationship to Existing Code

The `pipeline.py` script is a **simplified reimplementation** of your existing architecture:

| Existing Component | pipeline.py Equivalent | Status |
|--------------------|------------------------|--------|
| `tbl_publications` | `PipelineConfig.landing_page` | Simplified |
| `tbl_models` | `PipelineConfig.file_patterns` | Simplified |
| `tbl_model_columns` | `SheetMapping.column_mappings` | Simplified |
| `tbl_column_aliases` | Inline in `SheetMapping` | Merged |
| `tbl_queue` | Not needed (direct loading) | Removed |
| Agent state machine | CLI prompts | Removed |

### Migration Path

1. **Week 1**: Use `pipeline.py` to bootstrap 2-3 publications
2. **Week 2**: Verify auto-load works for new periods
3. **Week 3**: If needed, migrate pipeline configs to your existing schema
4. **Week 4**: Reintegrate LLM enrichment for column descriptions

The key difference: `pipeline.py` stores config as JSONB in one table, while your existing code normalises across 39 tables. The JSONB approach is simpler but less queryable. Choose based on whether you need to query pipeline configs or just load/save them.

## Why This Works

The bootstrap-to-autoload bridge works because:

1. **Single responsibility**: Bootstrap learns patterns, Scan applies them
2. **Explicit storage**: Column mappings saved with the pipeline, not inferred each time
3. **Idempotent loads**: `ON CONFLICT` clauses prevent duplicate data
4. **Schema evolution**: `ADD COLUMN IF NOT EXISTS` handles NHS changing columns

The existing code tried to do all this but the integration points were broken. This version keeps everything in one file so the handoffs are function calls, not database queries across multiple schemas.
