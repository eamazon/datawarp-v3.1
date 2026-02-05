# DataWarp v3.1 — Architecture & Technical Reference

> The definitive reference for understanding, maintaining, and extending DataWarp v3.1.
> For both humans and coding agents. Covers everything from scratch.

**Companion document:** `docs/mcp/DATAWARP_GUIDE.md` — operational guide (CLI usage, SQL verification, MCP setup, troubleshooting).

---

## Table of Contents

### Part 1: System Overview
- [1.1 What DataWarp Does](#11-what-datawarp-does)
- [1.2 Architecture at a Glance](#12-architecture-at-a-glance)
- [1.3 Key Numbers](#13-key-numbers)
- [1.4 The V3 Bug That Shaped Everything](#14-the-v3-bug-that-shaped-everything)
- [1.5 Data Lifecycle](#15-data-lifecycle)
- [1.6 Key Design Decisions](#16-key-design-decisions)
- [1.7 Glossary](#17-glossary)

### Part 2: Architecture Deep Dive
- [2.1 Package Map](#21-package-map)
- [2.2 Data Model](#22-data-model)
- [2.3 Discovery Pipeline](#23-discovery-pipeline)
- [2.4 FileExtractor & Header Detection](#24-fileextractor--header-detection)
- [2.5 The Loading Path (Critical)](#25-the-loading-path-critical)
- [2.6 Grain Detection Algorithm](#26-grain-detection-algorithm)
- [2.7 LLM Enrichment Pipeline](#27-llm-enrichment-pipeline)
- [2.8 Period Detection Algorithm](#28-period-detection-algorithm)
- [2.9 Schema Drift Detection](#29-schema-drift-detection)
- [2.10 ZIP & CSV Processing](#210-zip--csv-processing)
- [2.11 MCP Server](#211-mcp-server)
- [2.12 Observability](#212-observability)

### Part 3: Module Reference
- [3.1 Core Data Structures](#31-core-data-structures)
- [3.2 Public API: discovery](#32-public-api-discovery)
- [3.3 Public API: loader](#33-public-api-loader)
- [3.4 Public API: metadata](#34-public-api-metadata)
- [3.5 Public API: pipeline](#35-public-api-pipeline)
- [3.6 Public API: utils](#36-public-api-utils)
- [3.7 Public API: storage](#37-public-api-storage)
- [3.8 Public API: transform](#38-public-api-transform)
- [3.9 Public API: tracking](#39-public-api-tracking)
- [3.10 CLI Commands Summary](#310-cli-commands-summary)
- [3.11 MCP Tools Summary](#311-mcp-tools-summary)
- [3.12 Database Views](#312-database-views)
- [3.13 Configuration Constants](#313-configuration-constants)
- [3.14 Environment Variables](#314-environment-variables)

### Part 4: Integration & Extension Guide
- [4.1 Integration Points for v3.2 Chatbot](#41-integration-points-for-v32-chatbot)
- [4.2 Adding a New Entity Type (Grain)](#42-adding-a-new-entity-type-grain)
- [4.3 Adding a New CLI Command](#43-adding-a-new-cli-command)
- [4.4 Adding a New MCP Tool](#44-adding-a-new-mcp-tool)
- [4.5 Supporting a New File Format](#45-supporting-a-new-file-format)
- [4.6 Supporting a New LLM Provider](#46-supporting-a-new-llm-provider)
- [4.7 Testing Strategy](#47-testing-strategy)
- [4.8 Common Agent Tasks (Cookbook)](#48-common-agent-tasks-cookbook)
- [4.9 Code Quality Rules](#49-code-quality-rules)
- [4.10 Document Map](#410-document-map)

---

# Part 1: System Overview

## 1.1 What DataWarp Does

DataWarp v3.1 is an NHS data pipeline that solves a specific problem: **NHS publishes hundreds of Excel/CSV datasets monthly, but the files have cryptic column names (ORG_CODE, MEASURE_1) and no machine-readable context.** A chatbot can't meaningfully query data it doesn't understand.

DataWarp automates the full journey:

1. **Discovers** Excel/CSV/ZIP files from NHS Digital and NHS England landing pages
2. **Detects entity grain** — whether data is at ICB, Trust, GP Practice, Region, or National level
3. **Enriches with LLM** — generates semantic column names and descriptions (org_code → icb_code, measure_1 → referrals_received)
4. **Loads to PostgreSQL** with period tracking — each monthly release appends to the same table
5. **Exposes via MCP** — Claude Desktop can query the data with full clinical context

The end result: a user asks Claude "What ADHD data do you have?" and gets an informed answer with context-aware SQL, because Claude sees semantic names, KPI definitions, and clinical methodology — not just raw columns.

## 1.2 Architecture at a Glance

```
  NHS DIGITAL                    DATAWARP                         POSTGRESQL
  ───────────                   ────────                         ──────────
       │                            │                                 │
       │  ┌───────────────────────────────────────────────────────┐   │
       │  │                  CLI COMMANDS                          │   │
       │  │  ┌─────────┐ ┌──────┐ ┌────────┐ ┌───────┐ ┌────────┐ │   │
       │  │  │bootstrap│ │ scan │ │backfill│ │ reset │ │ enrich │ │   │
       │  │  └────┬────┘ └──┬───┘ └───┬────┘ └───┬───┘ └───┬────┘ │   │
       │  └───────┼─────────┼─────────┼──────────┼─────────┼──────┘   │
       │          │         │         │          │         │          │
       ▼          ▼         ▼         ▼          │         │          │
  ┌─────────┐  ┌─────────────────────────┐       │         │          │
  │ Landing │  │      DISCOVERY          │       │         │          │
  │  Page   │──│  classifier.py          │       │         │          │
  │         │  │  scraper.py             │       │         │          │
  │ - Files │  │  - URL classification   │       │         │          │
  │ - Links │  │  - File discovery       │       │         │          │
  │ - Dates │  │  - Period extraction    │       │         │          │
  └─────────┘  └───────────┬─────────────┘       │         │          │
                           │                     │         │          │
                           ▼                     │         │          │
               ┌───────────────────────┐         │         │          │
               │      METADATA         │         │         ▼          │
               │  grain.py             │         │    ┌─────────┐     │
               │  enrich.py            │◀────────┼────│  LLM    │     │
               │  file_context.py      │         │    │ (Gemini)│     │
               │  - Grain detection    │         │    └─────────┘     │
               │  - LLM enrichment     │         │         │          │
               │  - Context extraction │         │         │          │
               └───────────┬───────────┘         │         │          │
                           │                     │         │          │
                           ▼                     │         │          │
               ┌───────────────────────┐         │         │          │
               │       LOADER          │         │         │          │
               │  excel.py             │         │         │          │
               │  extractor.py         │         │         │          │
               │  - Download files     │         │         │          │
               │  - Detect headers     │         │         │          │
               │  - Sanitize columns   │         │         │          │
               │  - Load to PostgreSQL │─────────┼─────────┼──────────┼───▶
               └───────────┬───────────┘         │         │          │
                           │                     │         │          │
                           ▼                     ▼         │          │
               ┌───────────────────────┐  ┌───────────┐    │          ▼
               │      PIPELINE         │  │  Config   │    │    ┌──────────┐
               │  config.py            │◀─│  (JSONB)  │────┼───▶│ datawarp │
               │  repository.py        │  └───────────┘    │    │ schema   │
               │  - Save/load config   │                   │    └──────────┘
               │  - Record history     │                   │          │
               └───────────────────────┘                   │          ▼
                                                           │    ┌──────────┐
               ┌───────────────────────┐                   │    │ staging  │
               │      MCP SERVER       │                   │    │ schema   │
               │  mcp_server.py        │◀──────────────────┘    └──────────┘
               │  - list_datasets      │                              │
               │  - get_schema         │                              │
               │  - query              │                              │
               │  - get_lineage        │                              │
               └───────────┬───────────┘                              │
                           │                                          │
                           ▼                                          │
               ┌───────────────────────┐                              │
               │    CLAUDE DESKTOP     │◀─────────────────────────────┘
               │  - Natural language   │
               │  - Context-aware SQL  │
               └───────────────────────┘
```

## 1.3 Key Numbers

| Metric | Value |
|--------|-------|
| Total Python source | 6,119 lines across 37 modules |
| Packages | 8 (`discovery`, `loader`, `metadata`, `pipeline`, `cli`, `utils`, `storage`, `transform`) |
| Config tables | 4 (`tbl_pipeline_configs`, `tbl_load_history`, `tbl_enrichment_log`, `tbl_cli_runs`) |
| Metadata views | 5 (`v_table_metadata`, `v_column_metadata`, `v_table_stats`, `v_tables`, `v_load_reconciliation`) |
| CLI commands | 8 (`bootstrap`, `scan`, `backfill`, `reset`, `enrich`, `add-sheet`, `list`, `history`) |
| MCP tools | 5 (`list_datasets`, `get_schema`, `query`, `get_periods`, `get_lineage`) |
| Entity types detected | 8 (`trust`, `icb`, `sub_icb`, `local_authority`, `gp_practice`, `ccg`, `region`, `national`) |
| Dependencies | 10 core packages (pandas, openpyxl, psycopg2, requests, beautifulsoup4, rich, click, litellm, python-dotenv, python-dateutil) |

## 1.4 The V3 Bug That Shaped Everything

V3 had 27,000 lines of code, 39 database tables, and **loaded zero rows**. The root cause: column names were generated separately for DDL (CREATE TABLE) and for the COPY command (data insertion). They drifted apart.

```
  V3 BUG (column names generated separately):

    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │ DDL Generator│      │ COPY Command │      │   Database   │
    │ (columns A)  │      │ (columns B)  │      │  (expects A) │
    └──────┬───────┘      └──────┬───────┘      └──────────────┘
           │                     │                     ▲
           │  "org_code,        │  "organisation_     │  MISMATCH!
           │   measure_1"       │   code, metric_1"   │  → 0 rows
           └─────────────────────┴─────────────────────┘


  V3.1 FIX (DataFrame is single source of truth):

    ┌──────────────────────────────────────────────────────────────┐
    │                    DataFrame.columns                          │
    │           ['icb_code', 'referrals', 'period']                │
    └────────────────────────┬─────────────────────────────────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ CREATE TABLE │  │ COPY FROM    │  │   Database   │
    │ (uses same   │  │ STDIN (uses  │  │   receives   │
    │  df.columns) │  │ df.columns)  │  │   matching   │
    └──────────────┘  └──────────────┘  └──────────────┘
           │                 │                 │
           └─────────────────┴─────────────────┘
                             │
                      ALL USE df.columns
                      (cannot drift)
```

**This is the most important architectural decision in the codebase.** Every code path that touches column names MUST read from `df.columns`, never generate names independently.

```python
# The V3.1 pattern (src/datawarp/loader/excel.py)
df.columns = [sanitize_name(c) for c in df.columns]  # Set ONCE
ddl = f'CREATE TABLE ({", ".join(df.columns)})'       # Read from truth
copy = f'COPY ({", ".join(df.columns)}) FROM STDIN'   # Read from truth
```

## 1.5 Data Lifecycle

DataWarp follows a learn-once, run-many lifecycle:

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

**The key insight:** `bootstrap` calls the LLM once to learn semantic names. All subsequent `scan` and `backfill` operations reuse those mappings — no LLM cost. `reset` clears data but preserves the learned enrichment, so you can reload without paying for LLM calls again.

## 1.6 Key Design Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | **JSONB config storage** | Entire `PipelineConfig` in one JSONB column. No 39-table schema sprawl. Views extract what's needed. |
| 2 | **DataFrame is truth** | DDL and COPY both use `df.columns`. Prevents column drift that broke V3. |
| 3 | **Grain detection before enrichment** | Skip useless sheets (Notes, Methodology) early. Don't waste LLM tokens. |
| 4 | **Append-only loading** | `period` column tracks data across monthly releases. Same table, different periods. |
| 5 | **Enrichment returns dict** | Single application point for column renaming. No scattered name-generation logic. |
| 6 | **Table name collision prevention** | `_TableNameRegistry` ensures unique names per bootstrap session. |
| 7 | **Source row reconciliation** | `v_load_reconciliation` compares source file rows vs DB rows. Detect data loss immediately. |
| 8 | **CSV/XLSX deduplication** | ZIPs often contain both formats. Keep XLSX (richer), skip CSV. |
| 9 | **Two-stage enrichment** | Stage 1: Extract context from Notes/Contents sheets. Stage 2: Enrich each data sheet with that context. Better names. |
| 10 | **Provisional → final replacement** | NHS releases current month as provisional, previous as final. Scan always reloads 2 most recent periods. |
| 11 | **Schema fingerprinting for CSVs** | CSVs with identical column structure load to one table, not N tables. |
| 12 | **MCP exposes file_context** | Claude gets KPI definitions, clinical methodology, and data source notes — not just column names. |

## 1.7 Glossary

| Term | Meaning |
|------|---------|
| **Grain** | The entity level of the data: ICB, Trust, GP Practice, Region, National, etc. Detected from entity codes in the data. |
| **Period** | A YYYY-MM string representing which monthly/quarterly release the data comes from. E.g., `2024-11`. |
| **Enrichment** | The process of calling an LLM to generate semantic column names and descriptions from cryptic originals. |
| **Drift** | When a new monthly release has columns that didn't exist in previous releases. Auto-detected and auto-migrated. |
| **File context** | Structured metadata extracted from Notes/Contents/Definitions sheets in an Excel workbook. Contains KPI definitions, methodology, and clinical codes. |
| **Fingerprint** | Hash of a CSV's column structure, used to group files with identical schemas into one table. |
| **Provisional** | NHS's term for the current month's data, which may be revised. The previous month becomes "final". |
| **Pipeline** | A saved configuration (PipelineConfig) that knows how to discover, parse, and load data from one NHS publication. |
| **Sheet mapping** | Configuration for one sheet within a file: column mappings, descriptions, grain, target table name. |
| **Bootstrap** | The initial setup command that discovers files, learns patterns, and creates a pipeline configuration. |
| **Scan** | The recurring command that finds new periods and loads them using saved mappings (no LLM cost). |
| **MCP** | Model Context Protocol — the standard for exposing tools and data to Claude Desktop. |

---

# Part 2: Architecture Deep Dive

## 2.1 Package Map

```
src/datawarp/                              # Root package
├── __init__.py                    (2)     # Version: __version__ = "3.1"
│
├── storage/                               # Database connectivity
│   ├── __init__.py                (2)     # Re-exports get_connection
│   └── connection.py             (55)     # PostgreSQL context manager
│
├── utils/                                 # Shared utilities
│   ├── __init__.py                (3)     # Re-exports parse_period, sanitize_name
│   ├── sanitize.py               (70)     # Column/table name sanitization
│   └── period.py                (306)     # Period parsing (YYYY-MM extraction)
│
├── discovery/                             # NHS URL scraping & classification
│   ├── __init__.py                (8)     # Re-exports scrape_landing_page, classify_url
│   ├── scraper.py               (203)     # HTML scraping for file URLs
│   └── classifier.py            (388)     # URL classification & template detection
│
├── loader/                                # File loading to PostgreSQL
│   ├── __init__.py               (14)     # Re-exports load_file, FileExtractor, etc.
│   ├── excel.py                 (513)     # Download, load, drift detection
│   └── extractor.py             (726)     # Multi-row header detection, type inference
│
├── metadata/                              # Grain detection, LLM enrichment
│   ├── __init__.py                (5)     # Re-exports detect_grain, enrich_sheet
│   ├── grain.py                 (256)     # Entity type detection from data values
│   ├── enrich.py                (288)     # LLM enrichment via LiteLLM
│   ├── inference.py             (222)     # Heuristic metadata (no LLM)
│   ├── file_context.py          (149)     # Extract context from Notes/Contents sheets
│   ├── column_compressor.py     (126)     # Compress timeseries columns for LLM
│   └── canonicalize.py          (143)     # Remove date patterns, extract temporal qualifiers
│
├── pipeline/                              # Config persistence
│   ├── __init__.py                (3)     # Re-exports PipelineConfig, save_config, etc.
│   ├── config.py                (131)     # Dataclasses: PipelineConfig, FilePattern, SheetMapping
│   └── repository.py            (115)     # CRUD operations on tbl_pipeline_configs
│
├── transform/                             # Data transformations
│   ├── __init__.py               (17)     # Re-exports detect_and_unpivot
│   └── unpivot.py                (97)     # Unpivot wide date-as-column formats
│
├── cli/                                   # CLI commands (Click framework)
│   ├── __init__.py               (41)     # Registers all commands
│   ├── console.py                (24)     # Shared Rich console + theme
│   ├── bootstrap.py             (511)     # bootstrap command + _TableNameRegistry
│   ├── scan.py                  (171)     # scan command
│   ├── backfill.py               (92)     # backfill command
│   ├── enrich.py                (160)     # enrich command
│   ├── add_sheet.py             (202)     # add-sheet command
│   ├── reset.py                 (114)     # reset command
│   ├── list_history.py           (76)     # list + history commands
│   ├── helpers.py                (81)     # Shared CLI utilities
│   ├── file_processor.py        (392)     # Load files with enrichment + reconciliation
│   ├── sheet_selector.py        (143)     # Interactive sheet selection
│   └── schema_grouper.py        (116)     # Group & dedupe files by schema
│
└── tracking.py                  (154)     # CLI run tracking (eventstore pattern)
```

**Scripts (outside package):**

```
scripts/
├── pipeline.py                   (48)     # CLI entry point (Click group)
├── mcp_server.py                (600+)    # MCP server for Claude Desktop
└── reset_db.sh                            # Drop staging + truncate config tables
```

**Line counts in parentheses.** Files over 250 lines: `extractor.py` (726), `excel.py` (513), `bootstrap.py` (511), `file_processor.py` (392), `classifier.py` (388), `period.py` (306).

## 2.2 Data Model

### Config Hierarchy

The entire pipeline configuration is stored as a single JSONB blob in `datawarp.tbl_pipeline_configs`:

```
PipelineConfig
├── pipeline_id: "adhd"
├── name: "ADHD Referrals"
├── landing_page: "https://digital.nhs.uk/.../mi-adhd"
├── discovery_mode: "discover"        # or "template", "explicit"
├── url_pattern: null                  # Template: "{landing_page}/{month_name}-{year}"
├── frequency: "monthly"              # "monthly", "quarterly", "annual"
├── loaded_periods: ["2024-11", "2024-12"]
├── file_context:                      # From Notes/Contents sheets
│   ├── sheets: {"ICB Data": "ADHD referrals by ICB"}
│   ├── kpis: {"wait_4_weeks": "% waiting 4+ weeks from referral"}
│   ├── definitions: {"referral": "Initial contact record..."}
│   └── methodology: "Data from MHSDS submissions..."
│
└── file_patterns: [FilePattern]
    └── FilePattern
        ├── filename_patterns: ["ADHD.*\\.xlsx"]     # Regex list
        ├── file_types: ["xlsx"]
        └── sheet_mappings: [SheetMapping]
            └── SheetMapping
                ├── sheet_pattern: "ICB Data"
                ├── table_name: "tbl_adhd_icb_referrals"
                ├── table_description: "ADHD referrals by ICB"
                ├── grain: "icb"
                ├── grain_column: "icb_code"
                ├── grain_description: "Integrated Care Board level"
                ├── column_mappings:
                │   ├── "org_code" → "icb_code"
                │   ├── "measure_1" → "referrals_received"
                │   └── "rpt_dt" → "report_date"
                ├── column_descriptions:
                │   ├── "icb_code" → "ICB organisation code (e.g., QWE)"
                │   └── "referrals_received" → "Number of ADHD referrals received in period"
                ├── column_types:
                │   ├── "icb_code" → "VARCHAR(255)"
                │   └── "referrals_received" → "INTEGER"
                ├── mappings_version: 1        # Bumped on drift
                └── last_enriched: "2025-01-28T10:30:00"
```

### Database Schema (4 Tables + 5 Views)

**Full DDL:** `sql/schema.sql` (236 lines)

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `datawarp.tbl_pipeline_configs` | Pipeline configurations as JSONB | `pipeline_id` (PK), `config` (JSONB), `created_at`, `updated_at` |
| `datawarp.tbl_load_history` | Track every data load | `pipeline_id`, `period`, `table_name`, `sheet_name`, `rows_loaded`, `source_rows`, `source_path` |
| `datawarp.tbl_enrichment_log` | LLM call observability | `pipeline_id`, `provider`, `model`, `input_tokens`, `cost_usd`, `duration_ms`, `success` |
| `datawarp.tbl_cli_runs` | CLI command tracking | `command`, `args` (JSONB), `status`, `duration_ms`, `result_summary` (JSONB) |

| View | Purpose |
|------|---------|
| `datawarp.v_table_metadata` | Table names, descriptions, grain from JSONB config |
| `datawarp.v_column_metadata` | Column mappings and descriptions (one row per column) |
| `datawarp.v_table_stats` | Row counts and periods loaded per table |
| `datawarp.v_tables` | Combined metadata + stats (FULL OUTER JOIN) |
| `datawarp.v_load_reconciliation` | Source rows vs loaded rows with status (match/rows_lost/rows_added) |

**Two schemas in PostgreSQL:**
- `datawarp.*` — Configuration and tracking tables (static schema)
- `staging.*` — Dynamic data tables created during loading (one per sheet)

### Python → SQL Object Lifecycle

The entire system uses a single serialization boundary — Python objects ↔ JSONB. This eliminated the V3 problem where 39 normalized tables caused joins, drift, and inconsistency.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           PYTHON OBJECTS                                │
│                                                                         │
│  PipelineConfig                                                         │
│  ├── pipeline_id: "mi_adhd"                                             │
│  ├── name: "ADHD Referrals"                                             │
│  ├── file_patterns: [FilePattern, ...]                                  │
│  │   └── FilePattern                                                    │
│  │       └── sheet_mappings: [SheetMapping, ...]                        │
│  │           └── SheetMapping                                           │
│  │               ├── table_name: "tbl_mi_adhd_icb_referrals"           │
│  │               ├── column_mappings: {"org_code": "icb_code"}         │
│  │               ├── column_descriptions: {"icb_code": "ICB identifier"}│
│  │               └── grain: "icb"                                      │
│  └── loaded_periods: ["2025-11"]                                        │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │ .to_dict() / json.dumps()
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATABASE: datawarp schema                             │
│                                                                         │
│  tbl_pipeline_configs                                                   │
│  ┌──────────────┬──────────────────────────────────────────────────┐    │
│  │ pipeline_id  │ config (JSONB)                                   │    │
│  ├──────────────┼──────────────────────────────────────────────────┤    │
│  │ "mi_adhd"    │ {"pipeline_id": "mi_adhd",                       │    │
│  │              │  "file_patterns": [...],  ← ENTIRE OBJECT AS JSON│    │
│  │              │  "loaded_periods": ["2025-11"]}                  │    │
│  └──────────────┴──────────────────────────────────────────────────┘    │
│                                                                         │
│  tbl_load_history                                                       │
│  ┌─────────────┬─────────┬──────────────────────────┬───────┐          │
│  │ pipeline_id │ period  │ table_name               │ rows  │          │
│  ├─────────────┼─────────┼──────────────────────────┼───────┤          │
│  │ mi_adhd     │ 2025-11 │ tbl_mi_adhd_icb_referrals│ 8149  │          │
│  │ mi_adhd     │ 2025-12 │ tbl_mi_adhd_icb_referrals│ 8200  │          │
│  └─────────────┴─────────┴──────────────────────────┴───────┘          │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │ load_sheet() creates tables
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATABASE: staging schema                              │
│                                                                         │
│  tbl_mi_adhd_icb_referrals        (created dynamically)                 │
│  ┌──────────┬──────────┬───────────┬─────────┬─────────────┐           │
│  │ _row_id  │ icb_code │ icb_name  │referrals│ _period     │           │
│  ├──────────┼──────────┼───────────┼─────────┼─────────────┤           │
│  │ 1        │ QWE      │ South West│ 234     │ 2025-11     │           │
│  │ 2        │ QOP      │ North East│ 456     │ 2025-11     │           │
│  │ 3        │ QWE      │ South West│ 250     │ 2025-12     │  ← append │
│  └──────────┴──────────┴───────────┴─────────┴─────────────┘           │
└─────────────────────────────────────────────────────────────────────────┘
```

### Object Persistence Through the Lifecycle

Each pipeline phase has a different relationship with the Python ↔ SQL boundary:

| Phase | Direction | What Happens |
|-------|-----------|--------------|
| **Bootstrap** | Python → SQL | Objects built in memory, serialized to JSONB, staging tables created from df.columns |
| **Scan** | SQL → Python → SQL | Config loaded from JSONB, new files discovered, data appended using saved mappings |
| **MCP Query** | SQL → Python → User | Config loaded from JSONB, descriptions extracted, attached to query results |

### What Lives Where

| Object | Storage | Purpose |
|--------|---------|---------|
| `PipelineConfig` | `datawarp.tbl_pipeline_configs.config` (JSONB) | Everything about a publication |
| `FilePattern` | Nested in PipelineConfig JSONB | Which files to process |
| `SheetMapping` | Nested in FilePattern JSONB | Table name, column mappings, descriptions, grain |
| Load records | `datawarp.tbl_load_history` (relational) | Which periods loaded, row counts |
| Actual data | `staging.tbl_*` (dynamic) | The NHS data itself |

**2 config tables + N data tables** (where N = number of sheets across all publications)

## 2.3 Discovery Pipeline

Discovery turns an NHS URL into a list of downloadable files with period metadata.

### URL Classification (`src/datawarp/discovery/classifier.py`)

When given a URL, the classifier determines:

1. **Source:** NHS Digital (`digital.nhs.uk`) or NHS England (`england.nhs.uk`). Some NHS Digital pages redirect to England.
2. **Discovery mode:**
   - `template` — URL follows a predictable pattern like `{landing_page}/{month_name}-{year}`. Can generate URLs for any period.
   - `discover` — Must scrape the landing page to find files. Most common.
   - `explicit` — User provides direct file URLs. No automated discovery.
3. **Frequency:** `monthly`, `quarterly`, or `annual`.
4. **URL pattern:** For template mode, the pattern string (e.g., `{landing_page}/{month_name}-{year}`).

### Scraping (`src/datawarp/discovery/scraper.py`)

The scraper handles two NHS site structures:

- **NHS Digital (hierarchical):** Main page → sub-pages per period → files on each sub-page
- **NHS England (flat):** All files listed on one page

```
scrape_landing_page(url)
  │
  ├─ _scrape_page(url)              # Extract file links + sub-page links
  │   ├─ Find all <a> tags
  │   ├─ Filter by DATA_EXTENSIONS (.xlsx, .xls, .csv, .zip)
  │   ├─ Extract period from filename (parse_period)
  │   ├─ Extract period from URL path (extract_period_from_url)
  │   ├─ Inherit period from parent page if available
  │   └─ Find sub-page links (_is_subpage_link)
  │
  ├─ For each sub-page link:
  │   └─ _scrape_page(sub_url, inherit_period=page_period)
  │
  └─ Deduplicate by URL
     Return: List[DiscoveredFile]
```

**Period extraction priority chain** (first match wins):
1. Period in filename: `ADHD-Data-2024-11.xlsx` → `2024-11`
2. Period in URL path: `/january-2025/data-tables` → `2025-01`
3. Period inherited from parent page
4. None (file has no detectable period)

## 2.4 FileExtractor & Header Detection

**File:** `src/datawarp/loader/extractor.py` (726 lines) — the most sophisticated module.

NHS Excel files have complex structures: multi-row headers, merged cells, hierarchical column names, footnotes, and metadata mixed with data. The `FileExtractor` handles all of this.

### Sheet Classification

Every sheet is classified before processing:

| Type | Criteria | Action |
|------|----------|--------|
| `TABULAR` | Has detectable data rows with consistent columns | Process for loading |
| `METADATA` | Name matches patterns (Notes, Contents, Definitions, Glossary) | Skip for loading, extract context |
| `EMPTY` | No data rows or fewer than 2 columns | Skip |
| `UNRECOGNISED` | Cannot determine structure | Skip |

### Multi-Row Header Detection

```
FileExtractor.infer_structure()
  │
  ├─ _classify_sheet()           # TABULAR, METADATA, EMPTY, UNRECOGNISED
  ├─ _detect_all_header_rows()   # Find header row(s) by content patterns
  │   ├─ Row with mostly text values = potential header
  │   ├─ Adjacent text rows before data = multi-row header
  │   └─ Detects fiscal year rows, month-year headers
  ├─ _build_column_hierarchy()   # Handle merged cells → hierarchical names
  │   ├─ "Region > ICB > Referrals" from merged parent cells
  │   └─ ColumnInfo.original_headers preserves full hierarchy
  ├─ _detect_data_boundaries()   # Find where data starts and ends
  │   ├─ Skip footnotes (rows with mostly empty cells after data)
  │   └─ Skip repeated header rows mid-table
  └─ _infer_column_types()       # Map to PostgreSQL types
      ├─ Excel number format → INTEGER/NUMERIC/VARCHAR
      ├─ Date values → DATE or VARCHAR
      └─ Conservative: VARCHAR(255) for ambiguous columns
```

The `ColumnInfo` dataclass captures everything about each column:

```python
@dataclass
class ColumnInfo:
    excel_col: str          # "A", "B", etc.
    col_index: int          # 0-based
    pg_name: str            # PostgreSQL-safe name
    original_headers: List[str]  # ["Region", "ICB Code"] for merged headers
    inferred_type: str = 'VARCHAR(255)'
    sample_values: List[Any] = field(default_factory=list)
```

## 2.5 The Loading Path (Critical)

This is the most important code path in the system. Trace from file download to database row:

```
download_file(url)
  │ Returns: local file path
  ▼
FileExtractor(file_path, sheet_name)
  │ infer_structure() → TableStructure
  │ to_dataframe() → pd.DataFrame (raw column names)
  ▼
load_dataframe(df, table_name, period, column_mappings, sheet_mapping)
  │
  │ ═══ STEP 1: DRIFT DETECTION ═══
  │ detect_column_drift(df.columns, sheet_mapping)
  │   → new_cols: columns in data not in config
  │   → missing_cols: columns in config not in data
  │   → If drift: bump mappings_version, clear last_enriched
  │
  │ ═══ STEP 2: COLUMN MAPPING (THE CRITICAL FIX) ═══
  │ for orig_col in df.columns:
  │     sanitized = sanitize_name(str(orig_col))
  │     canonical = column_mappings.get(sanitized, sanitized)
  │     final_columns[orig_col] = canonical
  │ df = df.rename(columns=final_columns)    ◀── SINGLE SOURCE OF TRUTH
  │
  │ if period:
  │     df['period'] = period                ◀── Add period column
  │
  │ ═══ STEP 3: TYPE INFERENCE ═══
  │ for col in df.columns:
  │     pg_type = _infer_pg_type(df[col])    # INTEGER, NUMERIC, VARCHAR(255), etc.
  │
  │ ═══ STEP 4: CREATE TABLE (from df.columns) ═══
  │ CREATE TABLE IF NOT EXISTS staging.{table_name} (
  │     {col} {type} for col, type in zip(df.columns, types)
  │ )
  │
  │ ═══ STEP 5: SCHEMA EVOLUTION ═══
  │ For existing tables:
  │     Query information_schema.columns
  │     ALTER TABLE ADD COLUMN for any new columns
  │
  │ ═══ STEP 6: SMART REPLACE ═══
  │ DELETE FROM staging.{table} WHERE period = {period}
  │     (Replace, not append — handles provisional → final)
  │
  │ ═══ STEP 7: COPY DATA (from df.columns) ═══
  │ df.to_csv(buffer, sep='\t', na_rep='\\N')
  │ COPY staging.{table} ({df.columns}) FROM STDIN
  │
  ▼
record_load(pipeline_id, period, table_name, rows, source_rows)
  │ INSERT INTO datawarp.tbl_load_history
  ▼
Returns: (rows_loaded, column_mappings, column_types)
```

**Critical invariant:** Steps 4 and 7 both read from `df.columns`. They cannot drift apart because there is only one source.

## 2.6 Grain Detection Algorithm

**File:** `src/datawarp/metadata/grain.py` (256 lines)

Grain detection identifies what entity level the data represents. It uses a four-pass algorithm with decreasing specificity:

### Pass 1: Primary Org Columns (Low Threshold)

Look for columns named `org_code`, `provider_code`, `trust_code`, `geography_code`, etc. These columns in hierarchical tables may contain mixed entity types (some ICB codes, some region codes). Use a **low threshold (0.3)** to catch them.

### Pass 2: Standard Entity Detection (Normal Threshold)

Scan the first 10 columns (excluding measure columns like `count`, `total`, `referrals`). For each column, take 50 sample values and match against entity patterns using **normal threshold (0.5)**:

| Entity | Pattern | Examples |
|--------|---------|----------|
| `trust` | `^R[A-Z0-9]{1,4}$` | RJ1, RXH, R0A |
| `icb` | `^Q[A-Z0-9]{2}$` | QWE, QOP, QHG |
| `sub_icb` | `^([0-9]{2}[A-Z][A-Z0-9]{1,4}\|E54[0-9]{6})$` | 01A00, E54000027 |
| `local_authority` | `^E0[6-9][0-9]{6}$` | E09000008, E06000001 |
| `gp_practice` | `^[A-Z][0-9]{5}$` | A81001, B82001 |
| `ccg` | `^[0-9]{2}[A-Z]$` | 00J, 01A |
| `region` | `^(Y[0-9]{2}\|E40[0-9]{6})$` | Y56, E40000003 |
| `national` | `^E92[0-9]{6}$` | E92000001 |

### Pass 3: Name-Based Detection (Fallback)

If no code patterns match, look for entity names: "NHS TRUST", "INTEGRATED CARE BOARD", "LONDON BOROUGH", etc.

### Pass 4: National Keywords (Last Resort)

Check if data contains "ENGLAND", "NATIONAL", "TOTAL" — indicating national aggregate data.

### Resolution Logic

When both Pass 1 (primary org column) and Pass 2 (standard) produce results, prefer the primary org column match if its entity type has high priority (≥70). This handles hierarchical tables where the `org_code` column contains trust codes but a separate `region_code` column also matches.

## 2.7 LLM Enrichment Pipeline

**Files:** `src/datawarp/metadata/enrich.py`, `file_context.py`, `column_compressor.py`, `canonicalize.py`

Enrichment is a two-stage process:

### Stage 1: File Context Extraction

Before enriching individual sheets, extract context from the workbook's metadata sheets:

```
extract_metadata_text(file_path)
  │ Read sheets matching METADATA_PATTERNS:
  │   'contents', 'index', 'toc', 'notes', 'methodology',
  │   'definitions', 'glossary', 'data source', 'cover', 'about'
  │ Return: raw text (up to 50 rows per sheet)
  ▼
extract_file_context(metadata_text, all_sheets, pipeline_id, source_file)
  │ Call LLM once to extract structured data:
  │ Return: FileContext {
  │   sheets: {"ICB Data": "ADHD referrals by ICB"},
  │   kpis: {"wait_4_weeks": "% waiting 4+ weeks from referral"},
  │   definitions: {"referral": "Initial contact record..."},
  │   methodology: "Data from MHSDS submissions...",
  │   data_sources: ["MHSDS"],
  │   codes: {"F84.0": "Childhood autism (ICD-10)"}
  │ }
```

### Stage 2: Per-Sheet Enrichment

Each data sheet is enriched with context from Stage 1:

```
enrich_sheet(sheet_name, columns, sample_rows, publication_hint,
             grain_hint, pipeline_id, source_file, file_context)
  │
  ├─ compress_columns(columns)
  │   └─ If 30+ sequential columns (month01, month02, ...):
  │      Compress to samples + pattern description
  │      Reduces prompt tokens significantly
  │
  ├─ Build prompt with:
  │   - Sheet name, publication context, grain hint
  │   - Column list (compressed if needed)
  │   - Sample data (first 3 rows)
  │   - File context (KPI definitions, methodology)
  │
  ├─ litellm.completion(model=model_id, messages=[...])
  │
  ├─ Parse JSON response → {table_name, table_description, columns, descriptions}
  │
  ├─ expand_columns(result, pattern_info)
  │   └─ If columns were compressed, expand back to full set
  │
  ├─ remove_date_patterns(table_name)
  │   └─ "icb_referrals_2024" → "icb_referrals" (cross-period consistency)
  │
  ├─ extract_temporal_qualifier(sheet_description)
  │   └─ If sheet has intra-file temporal distinction (Q1/Q2/YTD),
  │      re-append qualifier: "icb_referrals_q1"
  │
  ├─ _log_enrichment_call(log_data)
  │   └─ INSERT INTO datawarp.tbl_enrichment_log (tokens, cost, timing, etc.)
  │
  └─ Return: {table_name, table_description, columns: {orig→semantic}, descriptions: {semantic→desc}}
```

**Fallback:** If the LLM call fails, `_fallback_enrichment()` returns identity mappings (original names preserved, empty descriptions).

**Cost tracking:** Every LLM call is logged to `tbl_enrichment_log` with input/output tokens, cost_usd, duration_ms, and the full prompt/response for auditing.

## 2.8 Period Detection Algorithm

**File:** `src/datawarp/utils/period.py` (306 lines)

`parse_period(text)` extracts a YYYY-MM period from any text string. It handles NHS-specific conventions:

### Priority Order (first match wins)

1. **Date ranges** → return END date:
   - "October 2019 - September 2025" → `2025-09`
   - Rationale: For cumulative data, the end date represents the latest coverage.

2. **Month-year patterns:**
   - "december-2025", "nov 2024", "January 2025" → `2025-01`
   - "31-december-2025" (with day) → `2025-12`

3. **ISO/structured formats:**
   - "2024-11", "2024_11", "2024/11" → `2024-11`

4. **Compact formats:**
   - "202411" (YYYYMM) → `2024-11`
   - "122025" (MMYYYY) → `2025-12`

5. **Abbreviated:**
   - "nov25" → `2025-11`

6. **Quarterly (NHS fiscal year):**
   - "q2-2526" → Quarter 2 of FY 2025-26 → `2025-07`
   - Mapping: Q1→April(04), Q2→July(07), Q3→October(10), Q4→January(01)

7. **Year-only (last resort):**
   - "2020" → `2020-01`

### Related Functions

- `parse_period_range(text)` → `(start_period, end_period)` tuple
- `extract_period_from_url(url)` → Extract from URL path segments
- `get_latest_period(periods)` → Most recent from list
- `sort_periods(periods, descending=True)` → Chronological sort

## 2.9 Schema Drift Detection

When a new monthly release adds columns that weren't in previous releases, that's "drift". DataWarp handles it automatically.

**Function:** `detect_column_drift()` in `src/datawarp/loader/excel.py`

```python
detect_column_drift(df_columns: List[str], sheet_mapping: SheetMapping) -> Dict:
    # Compare current DataFrame columns against saved column_mappings
    known_cols = set(sheet_mapping.column_mappings.keys())
    current_cols = set(sanitize_name(c) for c in df_columns)

    new_cols = current_cols - known_cols
    missing_cols = known_cols - current_cols

    return {
        'new_cols': new_cols,
        'missing_cols': missing_cols,
        'has_drift': bool(new_cols or missing_cols)
    }
```

**When drift is detected:**

1. New columns get identity mappings (name maps to itself)
2. New columns get empty descriptions (needs enrichment)
3. `mappings_version` is bumped (+1)
4. `last_enriched` is set to None (signals need for re-enrichment)
5. Config is saved with updated mappings
6. The database table gets `ALTER TABLE ADD COLUMN` for new columns

**Re-enrichment:** Run `enrich --pipeline <id>` to fill empty descriptions for drifted columns.

## 2.10 ZIP & CSV Processing

### CSV/XLSX Deduplication

NHS ZIP files often contain both CSV and XLSX versions of the same data. The deduplication logic:

1. Group files by base name (strip format suffixes like `_csv`, `_xlsx`)
2. Keep the richest format: XLSX > XLS > CSV
3. Skip duplicates

### Schema Fingerprinting for CSVs

**File:** `src/datawarp/cli/schema_grouper.py`

When a ZIP contains 72 CSVs with the same column structure (e.g., one per month), they should load to ONE table, not 72 tables.

```
group_by_schema(csv_files)
  │
  ├─ For each CSV:
  │   ├─ Read first row (headers)
  │   ├─ Compute fingerprint = hash(sorted(column_names))
  │   └─ Group by fingerprint
  │
  └─ Return: {fingerprint → [file_list]}
     Each group gets ONE enrichment, ONE table.
     All files in group load to same table with different periods.
```

### Table Name Collision Prevention

**Class:** `_TableNameRegistry` in `src/datawarp/cli/bootstrap.py`

During bootstrap, the LLM might suggest the same table name for different files. The registry prevents this:

```python
registry = _TableNameRegistry()
name1 = registry.register("icb_referrals", "file1.xlsx")  # → "icb_referrals"
name2 = registry.register("icb_referrals", "file2.xlsx")  # → "icb_referrals_file2"
```

## 2.11 MCP Server

**File:** `scripts/mcp_server.py` (600+ lines)

The MCP server is the output endpoint of the entire pipeline. It exposes NHS data to Claude Desktop with full semantic context.

### Tools

| Tool | Input | Output | Purpose |
|------|-------|--------|---------|
| `list_datasets` | `schema` (default: staging) | List of tables with descriptions, grain, row counts, periods | "What data do you have?" |
| `get_schema` | `table_name`, `schema` | Column names, types, descriptions, KPI definitions, methodology | "What columns does this table have?" |
| `query` | `sql` | Query results as JSON | "Show me ICB referrals for Nov 2025" |
| `get_periods` | `table_name` | List of available periods | "What time periods are available?" |
| `get_lineage` | `table_name` | Source pipeline, load history, enrichment history | "Where did this data come from?" |

### The "Gold Dust" — file_context

The most valuable thing the MCP server provides isn't the data — it's the **context**. When `get_schema` is called, it returns not just column names but also:

- **KPI definitions** from the workbook's Notes sheet
- **Clinical definitions** (what "referral" means in this context)
- **Methodology** (data sources, collection method)
- **Grain description** ("Integrated Care Board level data")

This enables Claude to write clinically-aware SQL queries, not just mechanical ones.

### Concrete Example: What Claude Sees

When a user asks "What ADHD data do you have?", Claude calls `list_datasets`:

```json
[
  {
    "table_name": "tbl_adhd_table_1",
    "description": "National ADHD referral summary statistics",
    "grain": "national",
    "row_count": 126,
    "pipeline_id": "mi_adhd"
  },
  {
    "table_name": "tbl_adhd_table_2a",
    "description": "ADHD referrals by ICB",
    "grain": "icb",
    "row_count": 8149,
    "pipeline_id": "mi_adhd"
  }
]
```

Claude then calls `get_schema` to understand the ICB table:

```json
{
  "table_name": "tbl_adhd_table_2a",
  "description": "ADHD referrals by ICB",
  "grain": "icb",
  "columns": [
    {"name": "icb_code", "type": "TEXT", "description": "ICB organisation code (e.g., QWE)"},
    {"name": "icb_name", "type": "TEXT", "description": "ICB organisation name"},
    {"name": "referrals_received", "type": "BIGINT", "description": "Number of new ADHD referrals"},
    {"name": "first_contacts", "type": "BIGINT", "description": "Number of first contacts made"},
    {"name": "_period", "type": "TEXT", "description": "Reporting period (YYYY-MM)"}
  ],
  "file_context": {
    "kpis": {"wait_4_weeks": "% waiting 4+ weeks from referral to first contact"},
    "methodology": "Data from MHSDS submissions by NHS trusts..."
  }
}
```

Now Claude can write intelligent, context-aware queries because it knows what the columns mean, what the KPIs measure, and where the data came from. This is what separates DataWarp from a raw data dump — **the metadata makes the data queryable by non-experts.**

### Read-Only SQL Enforcement

The `query` tool validates SQL to prevent destructive operations. Only SELECT queries are allowed.

## 2.12 Observability

### CLI Run Tracking (`src/datawarp/tracking.py`)

Every CLI command is wrapped in `track_run()`:

```python
with track_run('bootstrap', {'url': url}, pipeline_id) as tracker:
    # ... do work ...
    tracker['rows_loaded'] = 1000
    tracker['tables_created'] = ['tbl_adhd_icb']
# Automatically records success/failure, duration, result summary
```

Records go to `datawarp.tbl_cli_runs` with status transitions: `running` → `success` / `failed`.

**Graceful degradation:** If the database is unavailable, tracking silently skips (returns None for run_id). The pipeline still works.

### Enrichment Logging (`datawarp.tbl_enrichment_log`)

Every LLM call logs:
- Provider, model, prompt text, response text
- Input/output/total tokens, cost_usd
- Duration in milliseconds
- Success/failure with error message
- Column compression stats (original_column_count, compressed_column_count)

### Load Reconciliation (`datawarp.v_load_reconciliation`)

Compares source file row counts against loaded row counts:

```sql
SELECT table_name, source_rows, rows_loaded, reconciliation_status
FROM datawarp.v_load_reconciliation
WHERE reconciliation_status != 'match';
-- Expected: 0 rows (all match)
```

Status values: `match`, `rows_lost`, `rows_added`, `no_source_info`.

---

# Part 3: Module Reference

## 3.1 Core Data Structures

### PipelineConfig

**File:** `src/datawarp/pipeline/config.py:71`

```python
from datawarp.pipeline.config import PipelineConfig

@dataclass
class PipelineConfig:
    pipeline_id: str                              # "adhd"
    name: str                                     # "ADHD Referrals"
    landing_page: str                             # NHS URL
    file_patterns: List[FilePattern] = []         # File matching + sheet configs
    loaded_periods: List[str] = []                # ["2024-11", "2024-12"]
    auto_load: bool = False                       # Reserved for future auto-scan
    discovery_mode: str = 'discover'              # "template" | "discover" | "explicit"
    url_pattern: Optional[str] = None             # Template: "{landing_page}/{month_name}-{year}"
    frequency: str = 'monthly'                    # "monthly" | "quarterly" | "annual"
    file_context: Optional[Dict] = None           # FileContext from metadata sheets

    # Methods
    def to_dict(self) -> dict
    def to_json(self) -> str
    @classmethod
    def from_dict(cls, data: dict) -> 'PipelineConfig'
    @classmethod
    def from_json(cls, json_str: str) -> 'PipelineConfig'
    def add_period(self, period: str) -> None        # Mark period as loaded (sorted)
    def get_new_periods(self, available: List[str]) -> List[str]  # available - loaded
```

### FilePattern

**File:** `src/datawarp/pipeline/config.py:44`

```python
from datawarp.pipeline.config import FilePattern

@dataclass
class FilePattern:
    filename_patterns: List[str] = []             # Regex patterns: ["ADHD.*\\.xlsx"]
    file_types: List[str] = ['xlsx']              # Expected extensions
    sheet_mappings: List[SheetMapping] = []        # One per sheet in file

    # Methods
    def to_dict(self) -> dict
    @classmethod
    def from_dict(cls, data: dict) -> 'FilePattern'
    # Note: from_dict handles backward compat (old single 'filename_pattern' field)
```

### SheetMapping

**File:** `src/datawarp/pipeline/config.py:7`

```python
from datawarp.pipeline.config import SheetMapping

@dataclass
class SheetMapping:
    sheet_pattern: str                            # Sheet name or regex
    table_name: str                               # "tbl_adhd_icb"
    table_description: str = ""                   # "ADHD referrals by ICB"
    column_mappings: Dict[str, str] = {}          # source → canonical name
    column_descriptions: Dict[str, str] = {}      # canonical → description
    column_types: Dict[str, str] = {}             # canonical → PostgreSQL type
    grain: str = "unknown"                        # "icb", "trust", "national", etc.
    grain_column: Optional[str] = None            # Which column has entity codes
    grain_description: str = ""                   # "ICB level data"
    mappings_version: int = 1                     # Bumped on drift / re-enrichment
    last_enriched: Optional[str] = None           # ISO timestamp

    # Methods
    def to_dict(self) -> dict
    @classmethod
    def from_dict(cls, data: dict) -> 'SheetMapping'
```

### DiscoveredFile

**File:** `src/datawarp/discovery/scraper.py:13`

```python
from datawarp.discovery.scraper import DiscoveredFile

@dataclass
class DiscoveredFile:
    url: str                                      # Full download URL
    filename: str                                 # "ADHD-Data-2024-11.xlsx"
    file_type: str                                # "xlsx" | "csv" | "xls" | "zip"
    period: Optional[str]                         # "2024-11" or None
    title: Optional[str]                          # Link text or nearby heading
```

### URLClassification

**File:** `src/datawarp/discovery/classifier.py`

```python
from datawarp.discovery.classifier import URLClassification

@dataclass
class URLClassification:
    publication_id: str                           # "mi-adhd"
    name: str                                     # "MI ADHD"
    source: str                                   # "nhs_digital" | "nhs_england" | "unknown"
    landing_page: str                             # Canonical landing page URL
    discovery_mode: str                           # "template" | "discover" | "explicit"
    url_pattern: Optional[str]                    # Template string for URL generation
    frequency: str                                # "monthly" | "quarterly" | "annual"
    detected_periods: List[str]                   # Periods found during classification
    period_from: Optional[str]                    # Earliest detected
    period_to: Optional[str]                      # Latest detected
    is_landing_page: bool                         # True if URL is the main landing page
    has_hash: bool                                # True if URL has hash/anchor
    redirects_to_england: bool                    # NHS Digital → England redirect
    original_url: Optional[str]                   # URL before redirect resolution
    is_period_url: bool                           # True if URL targets a specific period
```

### TableStructure & ColumnInfo

**File:** `src/datawarp/loader/extractor.py`

```python
from datawarp.loader.extractor import TableStructure, ColumnInfo, SheetType

class SheetType(Enum):
    TABULAR = auto()
    METADATA = auto()
    EMPTY = auto()
    UNRECOGNISED = auto()

@dataclass
class ColumnInfo:
    excel_col: str                                # "A", "B", etc.
    col_index: int                                # 0-based column index
    pg_name: str                                  # PostgreSQL-safe name
    original_headers: List[str]                   # Multi-row header hierarchy
    inferred_type: str = 'VARCHAR(255)'           # PostgreSQL type
    sample_values: List[Any] = field(default_factory=list)

    @property
    def full_header(self) -> str:                 # "Region > ICB Code"

@dataclass
class TableStructure:
    sheet_name: str
    sheet_type: SheetType
    header_rows: List[int]                        # Row indices of headers
    data_start_row: int
    data_end_row: int
    columns: Dict[int, ColumnInfo]                # col_index → ColumnInfo
    error_message: Optional[str] = None

    @property
    def is_valid(self) -> bool                    # True if TABULAR + no errors
    @property
    def total_data_rows(self) -> int              # data_end - data_start + 1
    def get_column_names(self) -> List[str]       # List of pg_name in order
```

### FileContext

**File:** `src/datawarp/metadata/file_context.py`

```python
from datawarp.metadata.file_context import FileContext

@dataclass
class FileContext:
    sheets: Dict[str, str]                        # sheet_name → description
    kpis: Dict[str, str]                          # kpi_name → definition
    definitions: Dict[str, str]                   # measure → clinical definition
    methodology: str                              # Data collection methodology
    data_sources: List[str]                       # ["MHSDS", "HES"]
    codes: Dict[str, str]                         # code → meaning (SNOMED, ICD, etc.)
```

## 3.2 Public API: discovery

```python
from datawarp.discovery import scrape_landing_page, classify_url

# Scrape NHS landing page for downloadable data files
scrape_landing_page(
    url: str,                          # NHS landing page URL
    follow_links: bool = True          # Follow sub-page links (NHS Digital structure)
) -> List[DiscoveredFile]

# Classify URL to determine discovery strategy
classify_url(
    url: str                           # Any NHS URL
) -> URLClassification

# Generate period URLs from a template pattern
from datawarp.discovery.classifier import generate_period_urls
generate_period_urls(
    url_pattern: str,                  # "{landing_page}/{month_name}-{year}"
    landing_page: str,                 # Base URL
    start_period: str,                 # "2024-01"
    end_period: str                    # "2025-12"
) -> List[str]
```

## 3.3 Public API: loader

```python
from datawarp.loader import (
    download_file, load_file, load_sheet, load_dataframe,
    detect_column_drift, extract_zip, list_zip_contents,
    FileExtractor, get_sheet_names, clear_workbook_cache
)

# Download file from URL to local directory
download_file(
    url: str,
    target_dir: Optional[str] = None   # Default: tempfile.mkdtemp()
) -> str                               # Returns: local file path

# Load Excel/CSV file to PostgreSQL (auto-detects format)
load_file(
    file_path: str,
    table_name: str,
    schema: str = 'staging',
    period: Optional[str] = None,
    sheet_name: Optional[str] = None,  # For Excel: specific sheet
    column_mappings: Optional[Dict[str, str]] = None,
    sheet_mapping: Optional[SheetMapping] = None  # For drift detection
) -> Tuple[int, Dict[str, str], Dict[str, str]]
# Returns: (rows_loaded, column_mappings, column_types)

# Load specific Excel sheet
load_sheet(
    file_path: str,
    sheet_name: str,
    table_name: str,
    schema: str = 'staging',
    period: Optional[str] = None,
    column_mappings: Optional[Dict[str, str]] = None,
    sheet_mapping: Optional[SheetMapping] = None
) -> Tuple[int, Dict[str, str], Dict[str, str]]

# Load DataFrame directly to PostgreSQL
load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema: str = 'staging',
    period: Optional[str] = None,
    column_mappings: Optional[Dict[str, str]] = None,
    extractor_types: Optional[Dict[str, str]] = None,
    sheet_mapping: Optional[SheetMapping] = None
) -> Tuple[int, Dict[str, str], Dict[str, str]]

# Compare DataFrame columns against saved mappings
detect_column_drift(
    df_columns: List[str],
    sheet_mapping: SheetMapping
) -> Dict[str, Any]
# Returns: {'new_cols': set, 'missing_cols': set, 'has_drift': bool}

# Extract ZIP and return data files
extract_zip(
    zip_path: str,
    target_dir: Optional[str] = None
) -> List[Tuple[str, str]]            # [(extracted_path, relative_path_in_zip)]

# List ZIP contents without extracting
list_zip_contents(zip_path: str) -> List[dict]
# Returns: [{'filename', 'path', 'size', 'file_type'}]

# FileExtractor for sophisticated header detection
class FileExtractor:
    def __init__(self, filepath: str, sheet_name: str)
    def infer_structure(self) -> TableStructure
    def to_dataframe(self) -> pd.DataFrame

# Get sheet names from Excel file
get_sheet_names(file_path: str) -> List[str]

# Clear openpyxl workbook cache (call after batch processing)
clear_workbook_cache() -> None
```

## 3.4 Public API: metadata

```python
from datawarp.metadata import detect_grain, enrich_sheet, get_table_metadata

# Detect entity type from DataFrame values
from datawarp.metadata.grain import detect_grain
detect_grain(
    df: pd.DataFrame                   # DataFrame with data
) -> Dict
# Returns: {"grain": "icb", "grain_column": "org_code", "confidence": 0.95, "description": "..."}

# LLM enrichment for semantic names
from datawarp.metadata.enrich import enrich_sheet
enrich_sheet(
    sheet_name: str,
    columns: List[str],
    sample_rows: List[Dict],
    publication_hint: str = "",
    grain_hint: str = "",
    pipeline_id: str = "",
    source_file: str = "",
    file_context: Optional[Dict] = None
) -> Dict
# Returns: {"table_name", "table_description", "columns": {orig→semantic}, "descriptions": {semantic→desc}}

# Extract raw text from metadata sheets
from datawarp.metadata.file_context import extract_metadata_text
extract_metadata_text(
    file_path: str,
    max_rows: int = 50
) -> str

# Extract structured context via LLM
from datawarp.metadata.file_context import extract_file_context
extract_file_context(
    metadata_text: str,
    all_sheets: List[str] = None,
    pipeline_id: str = "",
    source_file: str = ""
) -> Optional[FileContext]

# Compress timeseries columns for smaller LLM prompts
from datawarp.metadata.column_compressor import compress_columns, expand_columns
compress_columns(columns: List[str]) -> Tuple[List[str], Optional[Dict]]
expand_columns(compressed_result: Dict, pattern_info: Optional[Dict]) -> Dict

# Get column metadata from information_schema
from datawarp.metadata.inference import get_table_metadata
get_table_metadata(
    table_name: str,
    schema: str = 'staging'
) -> Dict

# Heuristic column description (no LLM)
from datawarp.metadata.inference import infer_column_description
infer_column_description(col_name: str) -> Optional[str]

# Remove date patterns from names
from datawarp.metadata.canonicalize import remove_date_patterns, extract_temporal_qualifier
remove_date_patterns(column_name: str) -> str
extract_temporal_qualifier(column_name: str) -> Optional[str]
```

## 3.5 Public API: pipeline

```python
from datawarp.pipeline import (
    PipelineConfig, FilePattern, SheetMapping,
    save_config, load_config, list_configs, delete_config,
    record_load, get_load_history
)

# Save or update pipeline configuration
save_config(config: PipelineConfig) -> None
# INSERT ... ON CONFLICT (pipeline_id) DO UPDATE

# Load pipeline configuration by ID
load_config(pipeline_id: str) -> Optional[PipelineConfig]

# List all pipeline configurations
list_configs() -> List[PipelineConfig]

# Delete a pipeline configuration
delete_config(pipeline_id: str) -> bool

# Record a successful data load
record_load(
    pipeline_id: str,
    period: str,
    table_name: str,
    source_file: str,
    sheet_name: Optional[str],
    rows_loaded: int,
    source_rows: Optional[int] = None,    # For reconciliation
    source_columns: Optional[int] = None,
    source_path: Optional[str] = None     # Path within ZIP
) -> None

# Get load history for a pipeline
get_load_history(pipeline_id: str) -> List[dict]
```

## 3.6 Public API: utils

```python
from datawarp.utils import parse_period, sanitize_name, make_table_name

# Sanitize any string to PostgreSQL-safe identifier
from datawarp.utils.sanitize import sanitize_name
sanitize_name(name: str) -> str
# "MI ADHD Data" → "mi_adhd_data"
# Lowercase, replace special chars with _, truncate to 63 chars

# Create table name from pipeline ID and sheet name
from datawarp.utils.sanitize import make_table_name
make_table_name(pipeline_id: str, sheet_name: str) -> str
# make_table_name("adhd", "ICB Level Data") → "tbl_adhd_icb_level_data"

# Create pipeline ID from publication name
from datawarp.utils.sanitize import make_pipeline_id
make_pipeline_id(name: str) -> str
# "MI ADHD Data" → "mi_adhd_data"

# Extract YYYY-MM period from any text
from datawarp.utils.period import parse_period
parse_period(text: str) -> Optional[str]
# "november-2024" → "2024-11"
# "October 2019 - September 2025" → "2025-09" (END date)
# "q2-2526" → "2025-07" (NHS FY Q2)
# "2020" → "2020-01" (year-only)

# Extract period range as tuple
from datawarp.utils.period import parse_period_range
parse_period_range(text: str) -> Tuple[Optional[str], Optional[str]]
# "October 2019 - September 2025" → ("2019-10", "2025-09")

# Extract period from URL path
from datawarp.utils.period import extract_period_from_url
extract_period_from_url(url: str) -> Optional[str]

# Get most recent period from list
from datawarp.utils.period import get_latest_period
get_latest_period(periods: List[str]) -> Optional[str]

# Sort periods chronologically
from datawarp.utils.period import sort_periods
sort_periods(periods: List[str], descending: bool = True) -> List[str]
```

## 3.7 Public API: storage

```python
from datawarp.storage import get_connection, test_connection

# Get PostgreSQL connection as context manager
get_connection() -> Generator[PgConnection, None, None]
# Usage:
#   with get_connection() as conn:
#       with conn.cursor() as cur:
#           cur.execute("SELECT 1")
# Auto-commits on success, rollback on exception

# Test database connectivity
test_connection() -> bool
```

**Connection string from environment:**
```
host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD}
```

## 3.8 Public API: transform

```python
from datawarp.transform import detect_and_unpivot

# Detect and unpivot wide date-as-column formats
detect_and_unpivot(
    df: pd.DataFrame
) -> Tuple[pd.DataFrame, dict]
# Detects columns that are dates (Apr-24, 2024-Q1, FY2024)
# Unpivots to: id_columns + period + value
# Returns: (unpivoted_df, {'unpivoted': True, 'date_columns': [...], 'id_columns': [...]})
```

## 3.9 Public API: tracking

```python
from datawarp.tracking import track_run, start_run, complete_run, fail_run

# Context manager for CLI command tracking (preferred)
@contextmanager
track_run(
    command: str,                      # "bootstrap", "scan", etc.
    args: Dict[str, Any],             # Command arguments
    pipeline_id: Optional[str] = None
) -> Generator[Dict[str, Any], None, None]
# Usage:
#   with track_run('bootstrap', {'url': url}, pipeline_id) as tracker:
#       tracker['rows_loaded'] = 1000
#   # Automatically records success/failure

# Low-level functions (prefer track_run)
start_run(command: str, args: Dict, pipeline_id: Optional[str] = None) -> Optional[int]
complete_run(run_id: Optional[int], result_summary: Optional[Dict] = None) -> None
fail_run(run_id: Optional[int], error_message: str, result_summary: Optional[Dict] = None) -> None
```

## 3.10 CLI Commands Summary

**Entry point:** `scripts/pipeline.py` — Click group registering all commands.

**Run with:** `PYTHONPATH=src python scripts/pipeline.py <command> [options]`

| Command | Required Options | Optional | File |
|---------|-----------------|----------|------|
| `bootstrap` | `--url <URL>` | `--id`, `--name`, `--enrich`, `--skip-unknown` | `cli/bootstrap.py` |
| `scan` | `--pipeline <ID>` | `--dry-run`, `--force-scrape` | `cli/scan.py` |
| `backfill` | `--pipeline <ID>`, `--from`, `--to` | `--dry-run` | `cli/backfill.py` |
| `reset` | `--pipeline <ID>` | `--period`, `--delete`, `--yes` | `cli/reset.py` |
| `enrich` | `--pipeline <ID>` | `--file`, `--dry-run`, `--force` | `cli/enrich.py` |
| `add-sheet` | `--pipeline <ID>`, `--file`, `--sheet` | `--enrich` | `cli/add_sheet.py` |
| `list` | (none) | | `cli/list_history.py` |
| `history` | `--pipeline <ID>` | `--limit` | `cli/list_history.py` |

**Full CLI usage with examples:** See `docs/mcp/DATAWARP_GUIDE.md` Section 14.

## 3.11 MCP Tools Summary

**Server:** `scripts/mcp_server.py`

| Tool | Parameters | Returns |
|------|-----------|---------|
| `list_datasets` | `schema` (default: staging) | Array of `{name, description, grain, row_count, periods, pipeline_id}` |
| `get_schema` | `table_name`, `schema` | `{columns: [{name, type, description}], grain, file_context, ...}` |
| `query` | `sql` | `{rows: [...], columns: [...], row_count}` |
| `get_periods` | `table_name` | `{periods: ["2024-11", "2024-12", ...]}` |
| `get_lineage` | `table_name` | `{source_pipeline, landing_page, load_history, enrichment_history}` |

**Test:** `PYTHONPATH=src python scripts/mcp_server.py --test`

**Claude Desktop config:** See `docs/mcp/DATAWARP_GUIDE.md` Section 13.

## 3.12 Database Views

### v_table_metadata

Extracts table-level metadata from the JSONB config. One row per table.

```sql
-- Columns: pipeline_id, publication_name, landing_page, table_name,
--          table_description, grain, grain_column, grain_description,
--          column_mappings, column_descriptions, mappings_version,
--          last_enriched, config_created, config_updated
SELECT * FROM datawarp.v_table_metadata;
```

**Use when:** You need to know what tables exist and their metadata.

### v_column_metadata

One row per column per table. Shows original → semantic name mapping.

```sql
-- Columns: pipeline_id, table_name, grain, original_name, semantic_name,
--          is_enriched, column_description, mappings_version, last_enriched
SELECT * FROM datawarp.v_column_metadata WHERE table_name = 'tbl_adhd_icb';
```

**Use when:** You need column-level detail or want to find unenriched columns.

### v_table_stats

Aggregate load statistics per table.

```sql
-- Columns: pipeline_id, table_name, periods_loaded, total_rows,
--          earliest_period, latest_period, last_loaded
SELECT * FROM datawarp.v_table_stats;
```

**Use when:** You need to know how much data is loaded.

### v_tables

Combined metadata + stats via FULL OUTER JOIN. The primary view for MCP's `list_datasets`.

```sql
-- All columns from v_table_metadata + v_table_stats
SELECT * FROM datawarp.v_tables;
```

### v_load_reconciliation

Data integrity check: compares source file rows against loaded rows.

```sql
-- Columns: pipeline_id, period, table_name, source_file, source_path,
--          source_rows, rows_loaded, reconciliation_status, row_difference, loaded_at
SELECT * FROM datawarp.v_load_reconciliation WHERE reconciliation_status != 'match';
-- Expected: 0 rows
```

## 3.13 Configuration Constants

### Entity Patterns (`src/datawarp/metadata/grain.py`)

```python
ENTITY_PATTERNS = {
    'trust':           {'pattern': r'^R[A-Z0-9]{1,4}$',                         'priority': 100},
    'icb':             {'pattern': r'^Q[A-Z0-9]{2}$',                           'priority': 100},
    'sub_icb':         {'pattern': r'^([0-9]{2}[A-Z][A-Z0-9]{1,4}|E54[0-9]{6})$', 'priority': 85},
    'local_authority': {'pattern': r'^E0[6-9][0-9]{6}$',                        'priority': 90},
    'gp_practice':     {'pattern': r'^[A-Z][0-9]{5}$',                          'priority': 100},
    'ccg':             {'pattern': r'^[0-9]{2}[A-Z]$',                          'priority': 70},
    'region':          {'pattern': r'^(Y[0-9]{2}|E40[0-9]{6})$',               'priority': 50},
    'national':        {'keywords': ['ENGLAND', 'NATIONAL', 'TOTAL', 'ALL'],   'priority': 10},
}
```

### Grain Detection Thresholds

```python
MIN_CONFIDENCE = 0.5           # Standard detection threshold
MIN_CONFIDENCE_ORG_COLUMN = 0.3  # Lower threshold for primary org columns
MIN_MATCHES = 3                # Minimum matching values (avoid false positives)
```

### Metadata Sheet Patterns (`src/datawarp/metadata/file_context.py`)

```python
METADATA_PATTERNS = (
    'contents', 'index', 'toc', 'notes', 'methodology', 'definitions',
    'glossary', 'data source', 'cover', 'about', 'title', 'key facts'
)
```

### Measure Keywords (excluded from grain detection)

```python
MEASURE_KEYWORDS = [
    'count', 'total', 'number', 'percent', 'rate', 'ratio', 'average', 'mean',
    'median', 'sum', 'referrals', 'waiting', 'deliveries', 'admissions', 'episodes',
    'attendances', 'appointments', 'caesarean', 'breaches', 'waits', 'patients'
]
```

### File Extensions

```python
DATA_EXTENSIONS = {'.xlsx', '.xls', '.csv', '.zip'}  # scraper.py
```

### Excluded Values (grain detection)

```python
EXCLUDE_VALUES = {
    'UNKNOWN', 'OTHER', 'UNSPECIFIED', 'N/A', 'NA', '-', '', 'NULL', 'NONE',
    'ALL PROVIDERS', 'ALL TRUSTS', 'ALL ICBS', 'SUPPRESSED', 'REDACTED', '*'
}
```

## 3.14 Environment Variables

| Variable | Default | Required | Used By |
|----------|---------|----------|---------|
| `POSTGRES_HOST` | `localhost` | Yes | `storage/connection.py` |
| `POSTGRES_PORT` | `5432` | No | `storage/connection.py` |
| `POSTGRES_DB` | `datawalker` | Yes | `storage/connection.py` |
| `POSTGRES_USER` | `databot` | Yes | `storage/connection.py` |
| `POSTGRES_PASSWORD` | (empty) | Yes | `storage/connection.py` |
| `LLM_PROVIDER` | `gemini` | For enrichment | `metadata/enrich.py` |
| `LLM_MODEL` | `gemini-2.0-flash-exp` | For enrichment | `metadata/enrich.py` |
| `GEMINI_API_KEY` | (none) | For Gemini enrichment | LiteLLM |
| `LLM_MAX_OUTPUT_TOKENS` | `2000` | No | `metadata/enrich.py` |
| `LLM_TEMPERATURE` | `0.1` | No | `metadata/enrich.py` |
| `LLM_TIMEOUT` | `60` | No | `metadata/enrich.py` |

---

# Part 4: Integration & Extension Guide

## 4.1 Integration Points for v3.2 Chatbot

The v3.2 chatbot (`docs/goagent/`) needs to query NHS data with context. Here's how to integrate:

### Option A: Via MCP (Recommended)

The MCP server already provides everything the chatbot needs:
- `list_datasets` → Show available tables
- `get_schema` → Get column descriptions, KPI definitions, methodology
- `query` → Execute SQL

This is the preferred path because it provides semantic context (file_context, grain descriptions) that a direct SQL connection doesn't.

### Option B: Direct Database Access

If the chatbot backend needs to bypass MCP:

```python
from datawarp.storage import get_connection
from datawarp.pipeline import list_configs, load_config

# Get all table metadata for LLM context
configs = list_configs()
for config in configs:
    for fp in config.file_patterns:
        for sm in fp.sheet_mappings:
            # sm.table_name, sm.table_description, sm.column_descriptions
            # config.file_context (KPI definitions, methodology)
            pass

# Or use the views:
with get_connection() as conn:
    cur = conn.cursor()
    cur.execute("SELECT * FROM datawarp.v_tables")  # Combined metadata + stats
```

### Key Data for Chatbot LLM Context

The chatbot's LLM prompt should include:
1. **Table descriptions** from `v_table_metadata.table_description`
2. **Column descriptions** from `v_column_metadata.column_description`
3. **Grain info** from `v_table_metadata.grain` + `grain_description`
4. **KPI definitions** from `PipelineConfig.file_context.kpis`
5. **Clinical definitions** from `PipelineConfig.file_context.definitions`
6. **Available periods** from `v_table_stats.earliest_period` / `latest_period`

## 4.2 Adding a New Entity Type (Grain)

To detect a new entity type (e.g., `pharmacy`):

**File:** `src/datawarp/metadata/grain.py`

1. Add pattern to `ENTITY_PATTERNS`:
```python
'pharmacy': {
    'pattern': r'^F[A-Z0-9]{4}$',           # Pharmacy code pattern
    'description': 'Community Pharmacy level',
    'examples': ['FA001', 'FA123'],
    'priority': 90
},
```

2. If entity has name-based detection, add to `NAME_PATTERNS`:
```python
'pharmacy': {
    'keywords': ['PHARMACY', 'CHEMIST', 'DISPENSING'],
    'description': 'Community Pharmacy (by name)',
    'priority': 75
},
```

3. If there are primary org column names, add to `PRIMARY_ORG_COLUMN_PATTERNS`:
```python
'pharmacy code', 'pharmacy_code', 'dispensing code',
```

4. Add test cases to `tests/test_grain_detection.py`.

## 4.3 Adding a New CLI Command

**Convention:** Each command lives in its own file under `src/datawarp/cli/`.

1. Create `src/datawarp/cli/my_command.py`:

```python
"""My command - single purpose description."""
import click
from .console import console
from ..tracking import track_run
from ..pipeline import load_config

@click.command('my-command')
@click.option('--pipeline', required=True, help='Pipeline ID')
def my_command(pipeline: str):
    """One-line description for --help."""
    with track_run('my-command', {'pipeline': pipeline}, pipeline) as tracker:
        config = load_config(pipeline)
        if not config:
            console.print(f"Pipeline '{pipeline}' not found", style="red")
            return
        # ... implementation ...
        tracker['result_key'] = result_value
```

2. Register in `src/datawarp/cli/__init__.py`:
```python
from .my_command import my_command
```

3. Register in `scripts/pipeline.py`:
```python
from datawarp.cli import my_command
cli.add_command(my_command, name='my-command')
```

**Pattern notes:**
- Always wrap with `track_run()` for observability
- Use `console` from `cli.console` (not print)
- Load config with `load_config()` early, fail fast if not found
- Keep command function thin — extract business logic to `_impl()` if complex

## 4.4 Adding a New MCP Tool

**File:** `scripts/mcp_server.py`

1. Add tool definition in `list_tools()`:
```python
Tool(
    name="my_tool",
    description="What this tool does (for Claude to understand when to use it)",
    inputSchema={
        "type": "object",
        "properties": {
            "param1": {"type": "string", "description": "What param1 is"},
        },
        "required": ["param1"]
    }
)
```

2. Add handler in `call_tool()`:
```python
elif name == "my_tool":
    result = my_tool_function(arguments.get("param1"))
    return [TextContent(type="text", text=json.dumps(result, indent=2))]
```

3. Implement the function that queries the database:
```python
def my_tool_function(param1: str) -> dict:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT ... WHERE ... = %s", (param1,))
        # ...
    return result
```

4. Test with: `PYTHONPATH=src python scripts/mcp_server.py --test`

## 4.5 Supporting a New File Format

To support a new file format (e.g., `.parquet`):

1. **Discovery:** Add extension to `DATA_EXTENSIONS` in `src/datawarp/discovery/scraper.py`:
```python
DATA_EXTENSIONS = {'.xlsx', '.xls', '.csv', '.zip', '.parquet'}
```

2. **Loading:** Add handler in `load_file()` in `src/datawarp/loader/excel.py`:
```python
if file_path.endswith('.parquet'):
    df = pd.read_parquet(file_path)
    return load_dataframe(df, table_name, schema, period, column_mappings, sheet_mapping=sheet_mapping)
```

3. **ZIP extraction:** If the format can appear in ZIPs, add to `data_extensions` in `extract_zip()`.

## 4.6 Supporting a New LLM Provider

DataWarp uses **LiteLLM** for LLM abstraction. Most providers work out of the box:

1. Set environment variables:
```bash
export LLM_PROVIDER=openai
export LLM_MODEL=gpt-4o
export OPENAI_API_KEY=your_key
```

2. The model identifier is constructed in `src/datawarp/metadata/enrich.py`:
```python
if provider == 'gemini':
    model_id = f"gemini/{model}"
elif provider == 'openai':
    model_id = model                    # OpenAI doesn't need prefix
elif provider == 'anthropic':
    model_id = model
else:
    model_id = f"{provider}/{model}"    # Generic: "provider/model"
```

3. For a completely new provider not in LiteLLM, add an `elif` branch for the model_id construction.

**Cost tracking:** LiteLLM's `completion_cost()` handles pricing for known models. For unknown models, cost will be logged as None.

## 4.7 Testing Strategy

### Existing Tests

```
tests/
├── conftest.py                        # Pytest configuration
├── test_period.py                     # 20 tests for parse_period()
└── test_grain_detection.py            # Grain detection tests
```

**Run all tests:**
```bash
PYTHONPATH=src python -m pytest tests/ -v
```

### Philosophy

From CLAUDE.md: **"Test with real NHS URLs. Assert row counts, not just 'no errors.'"**

Good test:
```python
def test_bootstrap_loads_data():
    # Bootstrap from real NHS URL
    # Assert: rows_loaded > 0
    # Assert: table exists in staging schema
    # Assert: period column populated
```

Bad test:
```python
def test_bootstrap_runs():
    # Bootstrap from URL
    # Assert: no exceptions raised  ← Too weak
```

### Adding Integration Tests

For testing against a real database:

```python
import pytest
from datawarp.storage import get_connection, test_connection

@pytest.fixture
def db_connection():
    """Skip if database unavailable."""
    if not test_connection():
        pytest.skip("Database not available")
    yield

def test_load_and_verify(db_connection):
    from datawarp.loader import load_dataframe
    import pandas as pd

    df = pd.DataFrame({'col1': [1, 2, 3], 'period': ['2024-11'] * 3})
    rows, mappings, types = load_dataframe(df, 'test_table', period='2024-11')

    assert rows == 3

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.test_table")
        assert cur.fetchone()[0] == 3

        # Cleanup
        cur.execute("DROP TABLE IF EXISTS staging.test_table")
```

### V3 Failures — What Tests Must Prevent

Every test in DataWarp exists because of a real failure from V3. This regression table drives our testing priorities:

| V3 Symptom | Root Cause | Test That Catches It |
|------------|------------|---------------------|
| 0 rows loaded | DDL columns ≠ INSERT columns | Assert `COUNT(*) > 0` after every load |
| Columns drifted | Multiple code paths generating names | Assert DDL columns == DataFrame columns |
| Enrichment not applied | Mappings stored but not read correctly | Assert column names are semantic, not raw |
| Useless sheets loaded | No grain filtering | Assert grain ≠ 'unknown' for loaded tables |
| Scan recreated tables | No append logic | Assert row count increases after scan |
| MCP returned empty descriptions | Metadata in different tables than MCP read | Assert all column_descriptions non-empty |

### SQL Validation Suites

These SQL queries can be run directly against a live database to validate pipeline health. They form the basis for automated testing and are organized into 6 suites:

**Suite 1 — Bootstrap Verification:**
```sql
-- 1.1 Rows actually loaded (THE CRITICAL TEST)
SELECT sm.value->>'table_name' as table_name
FROM datawarp.tbl_pipeline_configs pc,
     jsonb_array_elements(pc.config->'file_patterns') fp,
     jsonb_array_elements(fp->'sheet_mappings') sm
WHERE pc.pipeline_id = '<pipeline_id>';
-- Then for each table: SELECT COUNT(*) FROM staging.<table_name>
-- FAIL if ANY row_count = 0

-- 1.2 Pipeline config saved
SELECT pipeline_id, config->>'name' as name,
       jsonb_array_length(config->'file_patterns') as file_patterns,
       jsonb_array_length(config->'loaded_periods') as periods
FROM datawarp.tbl_pipeline_configs WHERE pipeline_id = '<pipeline_id>';
-- FAIL if no row returned

-- 1.3 Load history recorded
SELECT period, table_name, rows_loaded
FROM datawarp.tbl_load_history WHERE pipeline_id = '<pipeline_id>';
-- FAIL if no rows or rows_loaded = 0
```

**Suite 2 — Column Integrity (DDL Bug Test):**
```sql
-- 2.1 Get DDL columns
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'staging' AND table_name = '<table_name>'
ORDER BY ordinal_position;

-- 2.2 Check no raw column names leaked through
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'staging' AND table_name = '<table_name>'
  AND (column_name LIKE 'unnamed%' OR column_name LIKE 'column%'
       OR column_name LIKE 'measure_%' OR column_name LIKE 'table_%');
-- FAIL if any rows returned (raw names leaked)
```

**Suite 3 — Enrichment Verification:**
```sql
-- Column descriptions populated
SELECT sm.value->>'table_name' as table_name,
       sm.value->'column_descriptions' as descs
FROM datawarp.tbl_pipeline_configs pc,
     jsonb_array_elements(pc.config->'file_patterns') fp,
     jsonb_array_elements(fp->'sheet_mappings') sm
WHERE pc.pipeline_id = '<pipeline_id>';
-- FAIL if descriptions are NULL or empty
```

**Suite 4 — Grain Detection:**
```sql
SELECT sm.value->>'table_name' as table_name,
       sm.value->>'grain' as grain,
       sm.value->>'grain_column' as grain_column
FROM datawarp.tbl_pipeline_configs pc,
     jsonb_array_elements(pc.config->'file_patterns') fp,
     jsonb_array_elements(fp->'sheet_mappings') sm
WHERE pc.pipeline_id = '<pipeline_id>';
-- FAIL if grain = 'unknown' for data tables
```

**Suite 5 — Scan Verification (run before/after scan):**
```sql
-- Before scan: record counts
SELECT COUNT(*) as rows_before FROM staging.<table_name>;
SELECT DISTINCT _period FROM staging.<table_name>;
-- Run scan, then:
SELECT COUNT(*) as rows_after FROM staging.<table_name>;
-- FAIL if rows_after <= rows_before (when new period existed)
-- FAIL if original periods disappeared
```

**Suite 6 — MCP Verification:**
```bash
# Server starts and returns data
PYTHONPATH=src python scripts/mcp_server.py --test
# FAIL if exit code ≠ 0 or descriptions empty
```

### Automatic Remediation Guide

When tests fail, use these targeted fixes:

| Failed Test | Fix |
|-------------|-----|
| ROWS LOADED = 0 | Check `load_sheet()` for column mismatch errors. Verify `df.columns` used for both DDL and COPY. Re-run with `--verbose`. |
| DDL ≠ DATA COLUMNS | **THE V3 bug.** Check `load_sheet()` function. Ensure `df.columns` assigned ONCE, used for both DDL and COPY. Never generate column names separately. |
| ENRICHMENT EMPTY | Check `enrich_sheet()` returned valid JSON. Verify API key set. Check fallback logic when API fails. |
| GRAIN = UNKNOWN for all | Check `detect_grain()` patterns match NHS codes. Verify sample data has entity codes (QWE, RJ1, etc.). May be methodology sheet — should be skipped. |
| SCAN REPLACED instead of APPENDED | Check for `DROP TABLE` in `load_sheet()`. Verify `CREATE TABLE IF NOT EXISTS` used. Check `_period` column distinguishes data. |
| MCP DESCRIPTIONS EMPTY | Check `get_schema()` reads from JSONB config. Verify `column_descriptions` populated during enrichment. |

### Database Reset for Clean State

```bash
./scripts/reset_db.sh
# Drops all staging tables, truncates config tables, keeps schema
```

## 4.8 Common Agent Tasks (Cookbook)

### "I need to add a column to a pipeline's config"

```python
from datawarp.pipeline import load_config, save_config

config = load_config("adhd")
for fp in config.file_patterns:
    for sm in fp.sheet_mappings:
        if sm.table_name == "tbl_adhd_icb":
            sm.column_mappings["new_col"] = "semantic_name"
            sm.column_descriptions["semantic_name"] = "Description of the column"
            sm.mappings_version += 1
save_config(config)
```

### "I need to debug why data isn't loading"

Checklist:
1. **Period detection:** `parse_period(filename)` returns None? → Check `utils/period.py`
2. **Grain detection:** `detect_grain(df)` returns "unknown"? → Sheet might be metadata, not data
3. **File not found in scrape:** Check `scrape_landing_page(url)` output → URL pattern might not match
4. **Columns mismatch:** Check `v_load_reconciliation` → Compare source_rows vs rows_loaded
5. **LLM enrichment failed:** Check `tbl_enrichment_log` → Look for `success=false` entries

```sql
-- Quick diagnostic queries
SELECT * FROM datawarp.tbl_load_history ORDER BY loaded_at DESC LIMIT 10;
SELECT * FROM datawarp.tbl_enrichment_log WHERE success = false ORDER BY created_at DESC;
SELECT * FROM datawarp.v_load_reconciliation WHERE reconciliation_status != 'match';
SELECT * FROM datawarp.tbl_cli_runs WHERE status = 'failed' ORDER BY started_at DESC;
```

### "I need to understand the current state of a pipeline"

```python
from datawarp.pipeline import load_config

config = load_config("adhd")
print(f"Name: {config.name}")
print(f"Landing page: {config.landing_page}")
print(f"Loaded periods: {config.loaded_periods}")
print(f"Discovery mode: {config.discovery_mode}")
for fp in config.file_patterns:
    print(f"  File patterns: {fp.filename_patterns}")
    for sm in fp.sheet_mappings:
        print(f"    Sheet: {sm.sheet_pattern} → {sm.table_name} (grain: {sm.grain}, v{sm.mappings_version})")
```

### "I need to add support for a new NHS publication"

1. Find the publication landing page on NHS Digital or England
2. Run: `python scripts/pipeline.py bootstrap --url "<landing_page>" --id <id> --enrich`
3. Verify: `python scripts/pipeline.py list`
4. Check data: `SELECT * FROM datawarp.v_load_reconciliation WHERE pipeline_id='<id>'`
5. Set up recurring: `python scripts/pipeline.py scan --pipeline <id>`

## 4.9 Code Quality Rules

**Full rules:** See `CLAUDE.md` (the canonical source).

Key rules for agents:

1. **Max 300 lines per file.** Split if larger.
2. **Max 50 lines per function.** Extract helpers for nested logic.
3. **Check before you write.** `grep -r "def <function_name>" src/` before creating utilities.
4. **One file, one purpose.** If the docstring has "and", split it.
5. **Don't duplicate, import.** Use the utilities listed in Section 3 of this document.
6. **Keep working code working.** Don't refactor unless you have a specific bug or feature blocked.

**Red flags — stop and reassess:**
- Creating new database tables beyond the 4 config tables
- File exceeding 300 lines
- Adding "manager", "factory", "orchestrator" classes
- Tests passing without asserting row counts
- Swallowing exceptions silently
- Database changes in code without updating `sql/schema.sql`

## 4.10 Document Map

| Document | Purpose | When to Read |
|----------|---------|--------------|
| `CLAUDE.md` | Project instructions, coding rules, session workflow | Start of every session |
| `docs/ARCHITECTURE.md` | **(this file)** Architecture, code reference, integration guide | Understanding the system, extending it |
| `docs/mcp/DATAWARP_GUIDE.md` | Operational guide: CLI usage, SQL queries, MCP setup, visual diagrams | Using the system day-to-day |
| `docs/tasks/CURRENT.md` | Active work tracking, known issues | Checking what's pending |
| `docs/goagent/README.md` | v3.2 chatbot overview, cost model, phases | Planning chatbot integration |
| `docs/goagent/DESIGN_PLAN.md` | v3.2 detailed design: architecture, UI, API, database | Implementing chatbot features |
| `sql/schema.sql` | Complete database DDL (tables, views, indexes, migrations) | Database changes |
| `README.md` | Quick start (5 steps) | First-time setup |
| `docs/archive/` | Historical design documents | Reference only |

---

*Last updated: 2026-02-05*
*Generated from DataWarp v3.1 source code analysis.*
