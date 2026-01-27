# Create DataWarp v3.1 MVP

Create a new repository `datawarp-v3.1` that implements an NHS data pipeline with MCP integration.

## What It Does

1. **Bootstrap**: User provides NHS publication URL → discovers files → groups by period → user selects latest month's files/sheets → loads to PostgreSQL → saves pattern for future
2. **Scan**: Checks URL for new periods → loads automatically using saved pattern
3. **MCP Server**: Exposes data to Claude with heuristic metadata (no LLM needed)

## Repository Structure

```
datawarp-v3.1/
├── README.md
├── requirements.txt
├── .env.example
├── sql/schema.sql
│
├── scripts/
│   ├── pipeline.py          # CLI: bootstrap, scan, backfill, list
│   └── mcp_server.py        # MCP server with list_datasets, get_schema, query tools
│
└── src/datawarp/
    ├── __init__.py
    ├── discovery/
    │   ├── __init__.py
    │   └── scraper.py       # Scrape NHS landing pages, extract file links
    ├── loader/
    │   ├── __init__.py
    │   └── excel.py         # Load Excel/CSV to PostgreSQL with pandas
    ├── pipeline/
    │   ├── __init__.py
    │   ├── config.py        # PipelineConfig, FilePattern, SheetMapping dataclasses
    │   └── repository.py    # Save/load pipeline configs, record loads
    ├── metadata/
    │   ├── __init__.py
    │   └── inference.py     # Heuristic column descriptions from patterns
    ├── storage/
    │   ├── __init__.py
    │   └── connection.py    # PostgreSQL connection from env vars
    └── utils/
        ├── __init__.py
        ├── period.py        # Parse YYYY-MM from filenames/URLs
        └── sanitize.py      # Create PostgreSQL-safe identifiers
```

## Database Schema (sql/schema.sql)

```sql
CREATE SCHEMA IF NOT EXISTS datawarp;
CREATE SCHEMA IF NOT EXISTS staging;

-- Pipeline configs stored as JSONB
CREATE TABLE datawarp.tbl_pipeline_configs (
    pipeline_id VARCHAR(63) PRIMARY KEY,
    config JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Track what's been loaded
CREATE TABLE datawarp.tbl_load_history (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(63),
    period VARCHAR(20) NOT NULL,
    table_name VARCHAR(63) NOT NULL,
    source_file TEXT,
    sheet_name VARCHAR(100),
    rows_loaded INT,
    loaded_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(pipeline_id, period, table_name, sheet_name)
);
```

## Core Data Structures (src/datawarp/pipeline/config.py)

```python
@dataclass
class SheetMapping:
    sheet_pattern: str              # "ICB Level" or regex
    table_name: str                 # "tbl_adhd_icb"
    column_mappings: Dict[str, str] # source_col -> canonical_col
    column_types: Dict[str, str]    # canonical_col -> pg_type

@dataclass
class FilePattern:
    filename_pattern: str           # Regex: r"ADHD-.*\.xlsx"
    file_types: List[str]           # ["xlsx"]
    sheet_mappings: List[SheetMapping]

@dataclass
class PipelineConfig:
    pipeline_id: str                # "adhd"
    name: str                       # "ADHD Referrals"
    landing_page: str               # NHS URL
    file_patterns: List[FilePattern]
    loaded_periods: List[str]       # ["2024-11", "2024-12"]
    auto_load: bool = False
```

## Critical Implementation: The Column Fix (src/datawarp/loader/excel.py)

The original codebase has a DDL bug where column names drift between components. Fix by using pandas as single source of truth:

```python
def load_sheet(file_path, sheet_name, table_name, schema, period, column_mappings, conn):
    # 1. Read with pandas
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # 2. Sanitise columns ONCE
    final_columns = {}
    for orig in df.columns:
        sanitized = sanitize_name(str(orig))
        canonical = column_mappings.get(sanitized, sanitized)
        final_columns[orig] = canonical
    
    # 3. Apply to DataFrame - this becomes the SINGLE SOURCE OF TRUTH
    df.columns = [final_columns[c] for c in df.columns if c in final_columns]
    
    # 4. Create DDL using df.columns
    for col in df.columns:
        col_defs.append(f'"{col}" {infer_type(df[col])}')
    
    # 5. COPY using same df.columns - CANNOT DRIFT
    cur.copy_expert(f"COPY table ({', '.join(df.columns)}) FROM STDIN", buffer)
```

## CLI Commands (scripts/pipeline.py)

```bash
# Bootstrap: learn pattern from latest period
python scripts/pipeline.py bootstrap --url "https://digital.nhs.uk/.../mi-adhd"

# Scan: find and load new periods
python scripts/pipeline.py scan --pipeline adhd

# Backfill: load all history
python scripts/pipeline.py backfill --pipeline adhd --from 2023-01

# List registered pipelines
python scripts/pipeline.py list
```

### Bootstrap Flow

```
1. Scrape URL → discover files
2. Group by period (95 files → 38 monthly snapshots)
3. Show: "Latest: 2024-12 (5 data files)"
4. User selects files to include
5. For each file: show sheets, user selects which to load
6. Load data, LEARN column mappings
7. Save PipelineConfig as JSONB
```

### Scan Flow

```
1. Load PipelineConfig from database
2. Scrape URL → discover current files
3. Compare: which periods are new?
4. For each new period:
   - Match files using saved filename_pattern
   - Load sheets using saved sheet_mappings
   - Apply saved column_mappings
5. Record in load_history
```

## MCP Server (scripts/mcp_server.py)

Expose 4 tools:

1. **list_datasets**: Show available tables with row counts and descriptions
2. **get_schema**: Column metadata with heuristic descriptions
3. **query**: SQL or natural language query
4. **get_periods**: Available time periods for a dataset

### Heuristic Metadata (no LLM needed)

```python
# NHS entity patterns - detect from sample values
NHS_ENTITIES = {
    'icb': {'pattern': r'^Q[A-Z]{2}$', 'desc': 'Integrated Care Board code'},
    'trust': {'pattern': r'^R[A-Z0-9]{2,4}$', 'desc': 'NHS Trust code'},
}

# Column name patterns
COLUMN_PATTERNS = {
    r'.*_count$': 'Count of {subject}',
    r'.*_rate$': 'Rate per population',
    r'.*referral.*': 'Referral metric',
}

# Known columns
KNOWN_COLUMNS = {
    'icb_code': 'Integrated Care Board identifier',
    'period': 'Reporting period (YYYY-MM)',
}
```

## Discovery (src/datawarp/discovery/scraper.py)

```python
def scrape_landing_page(url: str, follow_links: bool = True) -> List[DiscoveredFile]:
    """
    Scrape NHS landing page for data files.
    
    - Handles NHS Digital (hierarchical: main page → sub-pages per period)
    - Handles NHS England (flat: all files on one page)
    - Extracts: file_url, filename, file_type, period, title
    """
```

## Period Parsing (src/datawarp/utils/period.py)

```python
def parse_period(text: str) -> Optional[str]:
    """
    Extract YYYY-MM from text. Handles:
    - 2024-11, 2024_11
    - november-2024, nov-2024
    - 2024-november
    """
```

## Dependencies (requirements.txt)

```
pandas>=2.0
openpyxl>=3.1
psycopg2-binary>=2.9
requests>=2.31
beautifulsoup4>=4.12
rich>=13.0
python-dotenv>=1.0
```

## Environment (.env.example)

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=datawarp
DB_USER=postgres
DB_PASSWORD=
```

## Success Criteria

1. `bootstrap` creates tables with non-zero rows
2. `scan` detects new periods and loads them to same tables
3. `mcp_server.py --test` returns meaningful column descriptions
4. Data accumulates across periods (table not recreated each scan)

## Test Commands

```bash
# Test discovery
python -c "
from datawarp.discovery import scrape_landing_page
files = scrape_landing_page('https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd')
print(f'Found {len(files)} files')
"

# Bootstrap
python scripts/pipeline.py bootstrap --url "https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd"

# Verify data loaded
psql -c "SELECT table_name FROM information_schema.tables WHERE table_schema='staging'"

# Test MCP
python scripts/mcp_server.py --test
```

## Implementation Order

1. Create directory structure and boilerplate
2. Implement `utils/period.py` and `utils/sanitize.py`
3. Implement `storage/connection.py`
4. Implement `discovery/scraper.py`
5. Implement `loader/excel.py` with the column fix
6. Implement `pipeline/config.py` dataclasses
7. Implement `pipeline/repository.py` CRUD
8. Implement `scripts/pipeline.py` CLI with bootstrap flow
9. Add scan and backfill commands
10. Implement `metadata/inference.py`
11. Implement `scripts/mcp_server.py`
12. Test end-to-end
