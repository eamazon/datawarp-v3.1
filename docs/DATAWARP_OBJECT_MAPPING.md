# DataWarp v3.1 - Python ↔ SQL Object Mapping

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PYTHON OBJECTS                                  │
│                                                                             │
│  PipelineConfig                                                             │
│  ├── pipeline_id: "mi_adhd"                                                 │
│  ├── name: "ADHD Referrals"                                                 │
│  ├── landing_page: "https://..."                                            │
│  ├── file_patterns: [FilePattern, ...]                                      │
│  │   └── FilePattern                                                        │
│  │       ├── filename_pattern: r"adhd.*\.xlsx"                              │
│  │       └── sheet_mappings: [SheetMapping, ...]                            │
│  │           └── SheetMapping                                               │
│  │               ├── table_name: "tbl_mi_adhd_icb_referrals"               │
│  │               ├── column_mappings: {"org_code": "icb_code"}             │
│  │               ├── column_descriptions: {"icb_code": "ICB identifier"}   │
│  │               └── grain: "icb"                                          │
│  └── loaded_periods: ["2025-11"]                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ .to_dict() / json.dumps()
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATABASE: datawarp schema                            │
│                                                                             │
│  tbl_pipeline_configs                                                       │
│  ┌──────────────┬─────────────────────────────────────────────────────┐    │
│  │ pipeline_id  │ config (JSONB)                                      │    │
│  ├──────────────┼─────────────────────────────────────────────────────┤    │
│  │ "mi_adhd"    │ {"pipeline_id": "mi_adhd",                          │    │
│  │              │  "name": "ADHD Referrals",                          │    │
│  │              │  "file_patterns": [...],  ← ENTIRE OBJECT AS JSON   │    │
│  │              │  "loaded_periods": ["2025-11"]}                     │    │
│  └──────────────┴─────────────────────────────────────────────────────┘    │
│                                                                             │
│  tbl_load_history                                                           │
│  ┌─────────────┬─────────┬──────────────────────────┬───────┬───────┐      │
│  │ pipeline_id │ period  │ table_name               │ rows  │ loaded│      │
│  ├─────────────┼─────────┼──────────────────────────┼───────┼───────┤      │
│  │ mi_adhd     │ 2025-11 │ tbl_mi_adhd_icb_referrals│ 8149  │ 17:53 │      │
│  │ mi_adhd     │ 2025-11 │ tbl_mi_adhd_national     │ 42    │ 17:53 │      │
│  │ mi_adhd     │ 2025-12 │ tbl_mi_adhd_icb_referrals│ 8200  │ 18:30 │      │
│  └─────────────┴─────────┴──────────────────────────┴───────┴───────┘      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ load_sheet() creates tables
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATABASE: staging schema                             │
│                                                                             │
│  tbl_mi_adhd_icb_referrals        (created dynamically)                     │
│  ┌──────────┬──────────┬───────────┬─────────┬─────────────┬──────────┐    │
│  │ _row_id  │ icb_code │ icb_name  │referrals│ _period     │_loaded_at│    │
│  ├──────────┼──────────┼───────────┼─────────┼─────────────┼──────────┤    │
│  │ 1        │ QWE      │ South West│ 234     │ 2025-11     │ ...      │    │
│  │ 2        │ QOP      │ North East│ 456     │ 2025-11     │ ...      │    │
│  │ 3        │ QWE      │ South West│ 250     │ 2025-12     │ ...      │    │  ← same table, new period
│  └──────────┴──────────┴───────────┴─────────┴─────────────┴──────────┘    │
│                                                                             │
│  tbl_mi_adhd_national             (created dynamically)                     │
│  ┌──────────┬─────────────┬───────────┬─────────┐                          │
│  │ _row_id  │ metric_name │ value     │ _period │                          │
│  ├──────────┼─────────────┼───────────┼─────────┤                          │
│  │ 1        │ total_refs  │ 12345     │ 2025-11 │                          │
│  └──────────┴─────────────┴───────────┴─────────┘                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

## The Key Insight: JSONB vs Normalised

**V3 (broken)** had 39 tables:
```
tbl_publications → tbl_models → tbl_model_columns → tbl_column_aliases → tbl_queue → ...
```
Data spread across tables, joins required, drift possible.

**V3.1** has 2 config tables:
```
tbl_pipeline_configs (JSONB) → contains EVERYTHING about the pipeline
tbl_load_history            → tracks what's been loaded
```

## Python → SQL Flow

### 1. BOOTSTRAP: Python creates, SQL stores

```python
# Python: Build the object
config = PipelineConfig(
    pipeline_id="mi_adhd",
    name="ADHD Referrals",
    file_patterns=[
        FilePattern(
            filename_pattern=r"adhd.*\.xlsx",
            sheet_mappings=[
                SheetMapping(
                    table_name="tbl_mi_adhd_icb_referrals",
                    column_mappings={"org_code": "icb_code"},
                    column_descriptions={"icb_code": "ICB identifier"},
                    grain="icb"
                )
            ]
        )
    ]
)

# Python: Convert to dict
config_dict = config.to_dict()

# SQL: Store as JSONB
INSERT INTO datawarp.tbl_pipeline_configs (pipeline_id, config)
VALUES ('mi_adhd', '{"pipeline_id": "mi_adhd", ...}')
```

### 2. LOAD: Python creates staging table

```python
# Python: df.columns is source of truth
df.columns = ["icb_code", "icb_name", "referrals", "_period", "_loaded_at"]

# SQL: DDL generated from df.columns
CREATE TABLE staging.tbl_mi_adhd_icb_referrals (
    _row_id SERIAL PRIMARY KEY,
    "icb_code" TEXT,
    "icb_name" TEXT,
    "referrals" BIGINT,
    "_period" TEXT,
    "_loaded_at" TEXT
)

# SQL: COPY uses same df.columns
COPY staging.tbl_mi_adhd_icb_referrals (icb_code, icb_name, referrals, _period, _loaded_at)
FROM STDIN WITH CSV
```

### 3. SCAN: SQL → Python → SQL

```python
# SQL → Python: Load config
SELECT config FROM datawarp.tbl_pipeline_configs WHERE pipeline_id = 'mi_adhd'

# Python: Reconstruct object
config = PipelineConfig.from_dict(row['config'])

# Python: Access mappings
for fp in config.file_patterns:
    for sm in fp.sheet_mappings:
        column_mappings = sm.column_mappings  # {"org_code": "icb_code"}

# Python → SQL: Load new period using same mappings
# (DDL already exists, just INSERT)
COPY staging.tbl_mi_adhd_icb_referrals (...) FROM STDIN
```

### 4. MCP: SQL → Python → User

```python
# SQL → Python: Get config
config = load_pipeline("mi_adhd", conn)

# Python: Extract descriptions for MCP
for sm in config.file_patterns[0].sheet_mappings:
    table_info = {
        "table": sm.table_name,
        "description": sm.table_description,
        "grain": sm.grain,
        "columns": [
            {"name": col, "description": sm.column_descriptions.get(col)}
            for col in get_table_columns(sm.table_name)
        ]
    }

# MCP → User: 
# "tbl_mi_adhd_icb_referrals - ADHD referrals by ICB (ICB level)"
# Columns:
#   - icb_code: ICB identifier
#   - referrals: Number of referrals received
```

## Object Lifecycle

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           BOOTSTRAP                                       │
│                                                                          │
│  1. scrape_landing_page()     → List[DiscoveredFile]                     │
│  2. group_by_period()         → Dict[period, List[files]]                │
│  3. detect_grain(df)          → {"grain": "icb", ...}                    │
│  4. enrich_sheet(...)         → {"table_name": ..., "columns": ...}      │
│  5. load_sheet(...)           → creates staging.tbl_xxx, returns rows    │
│  6. SheetMapping(...)         → Python object with all metadata          │
│  7. FilePattern(...)          → wraps sheet mappings                     │
│  8. PipelineConfig(...)       → wraps everything                         │
│  9. save_pipeline(config)     → INSERT into tbl_pipeline_configs (JSONB) │
│ 10. record_load(...)          → INSERT into tbl_load_history             │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Config persisted as JSONB
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                              SCAN                                         │
│                                                                          │
│  1. load_pipeline(id)         → PipelineConfig from JSONB                │
│  2. scrape_landing_page()     → List[DiscoveredFile]                     │
│  3. get_loaded_periods()      → ["2025-11"] from tbl_load_history        │
│  4. find new periods          → ["2025-12"]                              │
│  5. match file patterns       → regex match against saved patterns       │
│  6. load_sheet(...)           → uses saved column_mappings               │
│     └── NO enrichment call    → mappings already in config               │
│  7. record_load(...)          → INSERT into tbl_load_history             │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Data accumulates in staging tables
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                              MCP QUERY                                    │
│                                                                          │
│  1. list_datasets()           → reads tbl_pipeline_configs               │
│     └── extracts table_name, table_description, grain from JSONB         │
│  2. get_schema(table)         → reads tbl_pipeline_configs               │
│     └── extracts column_descriptions from JSONB                          │
│  3. query(sql)                → executes against staging.tbl_xxx         │
└──────────────────────────────────────────────────────────────────────────┘
```

## Summary: What Lives Where

| Object | Storage | Purpose |
|--------|---------|---------|
| `PipelineConfig` | `datawarp.tbl_pipeline_configs.config` (JSONB) | Everything about a publication |
| `FilePattern` | Nested in PipelineConfig JSONB | Which files to process |
| `SheetMapping` | Nested in FilePattern JSONB | Table name, column mappings, descriptions, grain |
| Load records | `datawarp.tbl_load_history` (relational) | Which periods loaded, row counts |
| Actual data | `staging.tbl_*` (dynamic) | The NHS data itself |

**2 config tables + N data tables** (where N = number of sheets across all publications)
