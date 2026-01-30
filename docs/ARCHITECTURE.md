# DataWarp v3.1 Architecture

## Overview

DataWarp is an NHS data pipeline that discovers, enriches, and loads healthcare data for MCP-based natural language querying.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                   │
│                                                                         │
│   NHS URL → Discovery → Grain Detection → LLM Enrichment → Load → MCP  │
│                                                                         │
│   scraper.py   grain.py      enrich.py        excel.py    mcp_server.py│
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Component Architecture

### 1. Discovery Layer (`src/datawarp/discovery/`)

**Purpose:** Find data files from NHS landing pages.

| File | Lines | Responsibility |
|------|-------|----------------|
| `scraper.py` | 203 | Scrape landing pages, extract file URLs |
| `classifier.py` | 371 | Classify URLs, detect discovery mode |

**Key Types:**
```python
@dataclass
class DiscoveredFile:
    url: str           # Download URL
    filename: str      # e.g., "adhd-data-jan-2025.xlsx"
    file_type: str     # xlsx, csv, xls, zip
    period: str        # YYYY-MM or None
    title: str         # Link text or nearby heading

@dataclass
class URLClassification:
    publication_id: str      # e.g., "mi_adhd"
    name: str                # e.g., "ADHD Referrals"
    source: str              # nhs_digital, nhs_england
    landing_page: str        # Base URL
    discovery_mode: str      # template, discover, explicit
    url_pattern: str         # URL template for period generation
    frequency: str           # monthly, quarterly
    detected_periods: list   # Available periods found
```

**Discovery Modes:**
- `template` - NHS Digital with predictable URLs → generate from pattern
- `discover` - NHS England or hash URLs → scrape landing page
- `explicit` - User provides exact file URLs

**Period Detection Chain:**
```
1. Parse filename (e.g., "data-jan-2025.xlsx")
2. Parse URL path (e.g., "/january-2025/")
3. Parse link text (e.g., "January 2025 Data")
4. Inherit from parent page URL
```

---

### 2. Metadata Layer (`src/datawarp/metadata/`)

**Purpose:** Detect entity grain and enrich with semantic names.

| File | Lines | Responsibility |
|------|-------|----------------|
| `grain.py` | 100 | Detect ICB/Trust/GP/Region/National |
| `enrich.py` | 219 | LLM enrichment via LiteLLM |
| `inference.py` | 222 | Heuristic metadata (no LLM) |

**Entity Patterns:**
```python
ENTITY_PATTERNS = {
    'trust':       r'^R[A-Z0-9]{1,4}$',     # RJ1, RXH, R0A
    'icb':         r'^Q[A-Z0-9]{2}$',       # QWE, QOP, QHG
    'gp_practice': r'^[A-Z][0-9]{5}$',      # A81001, B82001
    'region':      r'^Y[0-9]{2}$',          # Y56, Y58
    'national':    keywords=['ENGLAND', 'NATIONAL', 'TOTAL']
}
```

**Grain Detection Flow:**
```
DataFrame → Scan first 3 columns → Match patterns → Return grain info

{
    "grain": "icb",
    "grain_column": "org_code",
    "description": "Integrated Care Board level data"
}
```

**LLM Enrichment Flow:**
```
Sheet + Sample Rows → LiteLLM API → Semantic Names

Input:  Sheet "ADHD ICB Referrals", columns ["ORG_CODE", "MEASURE_1"]
Output: {
    "table_name": "icb_adhd_referrals",
    "columns": {"org_code": "icb_code", "measure_1": "referrals"},
    "descriptions": {"icb_code": "Integrated Care Board identifier"}
}
```

---

### 3. Loader Layer (`src/datawarp/loader/`)

**Purpose:** Load data to PostgreSQL with correct column mapping.

| File | Lines | Responsibility |
|------|-------|----------------|
| `excel.py` | 402 | Load Excel/CSV with column fix |
| `extractor.py` | 716 | Extract data from various formats |

**The Critical Column Fix:**
```python
# DataFrame.columns is THE source of truth
df.columns = [sanitize_name(c) for c in df.columns]

# DDL uses df.columns
ddl = f'CREATE TABLE staging.{table} ({", ".join(df.columns)} ...)'

# COPY uses df.columns
copy = f'COPY staging.{table} ({", ".join(df.columns)}) FROM STDIN'
```

**Load Flow:**
```
1. Read file → DataFrame
2. Apply column mappings to df.columns
3. Add system columns (_row_id, _period, _loaded_at)
4. CREATE TABLE IF NOT EXISTS using df.columns
5. COPY data using df.columns
6. Record in tbl_load_history
```

**Supported Formats:**
- `.xlsx` - Excel 2007+ (via openpyxl)
- `.xls` - Legacy Excel (via xlrd)
- `.csv` - Comma-separated values
- `.zip` - Extract and process contents recursively

---

### 4. Pipeline Layer (`src/datawarp/pipeline/`)

**Purpose:** Manage pipeline configuration and load history.

| File | Lines | Responsibility |
|------|-------|----------------|
| `config.py` | 118 | Data classes for config |
| `repository.py` | 107 | Database CRUD operations |

**Config Hierarchy:**
```python
PipelineConfig
├── pipeline_id: str              # "mi_adhd"
├── name: str                     # "ADHD Referrals"
├── landing_page: str             # NHS URL
├── file_patterns: List[FilePattern]
│   └── FilePattern
│       ├── filename_pattern: str # Regex for files
│       └── sheet_mappings: List[SheetMapping]
│           └── SheetMapping
│               ├── table_name: str
│               ├── table_description: str
│               ├── column_mappings: Dict[str, str]
│               ├── column_descriptions: Dict[str, str]
│               ├── grain: str
│               └── grain_column: str
└── loaded_periods: List[str]     # ["2025-11", "2025-12"]
```

---

### 5. MCP Layer (`scripts/mcp_server.py`)

**Purpose:** Expose data to Claude via Model Context Protocol.

**Tools Provided:**
| Tool | Purpose |
|------|---------|
| `list_datasets` | Tables with descriptions, grain, row counts |
| `get_schema` | Column metadata with descriptions |
| `query` | Execute SQL and return results |
| `get_periods` | Available periods for a dataset |

**MCP Flow:**
```
User: "What's the ADHD referral rate by ICB?"
  ↓
Claude calls list_datasets → finds tbl_mi_adhd_icb_referrals
  ↓
Claude calls get_schema → gets column descriptions
  ↓
Claude writes SQL with context
  ↓
Claude calls query → returns results
```

---

## CLI Workflows

### Bootstrap (One-time per publication)

```
1. Scrape landing page for files
2. Group files by period
3. User selects period (default: latest)
4. For each file/sheet:
   a. Detect grain (ICB/Trust/GP/etc)
   b. Skip if grain=unknown (methodology sheets)
   c. Call LLM for enrichment (if --enrich)
   d. Load to staging schema
   e. Record in load_history
5. Save PipelineConfig with patterns
```

### Scan (Recurring)

```
1. Load saved PipelineConfig
2. Discover current files
3. Find new periods not in loaded_periods
4. Load using saved column mappings (no LLM)
5. Append to existing tables
6. Update loaded_periods
```

### Backfill (Historical)

```
1. Load saved PipelineConfig
2. Discover all available files
3. Load each period not already loaded
4. Use saved column mappings
```

---

## Error Handling

**Fail Fast Philosophy:**
```python
# BAD - silent failure
try:
    load_data()
except Exception:
    pass

# GOOD - log and re-raise
try:
    load_data()
except Exception as e:
    console.print(f"[red]Load failed: {e}[/]")
    raise
```

**Graceful Degradation:**
- LLM enrichment fails → Use fallback (identity mappings)
- Column mismatch detected → Log warning, skip problematic rows
- Period parsing fails → Use "unknown" period

---

## Observability

### Load History
```sql
SELECT pipeline_id, period, table_name, rows_loaded, loaded_at
FROM datawarp.tbl_load_history
ORDER BY loaded_at DESC;
```

### Enrichment Logs
```sql
SELECT pipeline_id, sheet_name, model,
       input_tokens, output_tokens, duration_ms, success
FROM datawarp.tbl_enrichment_log
ORDER BY created_at DESC;
```

### Data Verification
```sql
SELECT table_name,
       (SELECT COUNT(*) FROM staging.{table_name}) as rows
FROM information_schema.tables
WHERE table_schema = 'staging';
```
