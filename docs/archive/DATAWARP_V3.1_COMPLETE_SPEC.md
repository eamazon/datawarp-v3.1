# DataWarp v3.1 - Complete Spec with Enrichment

## What We're Building

NHS data pipeline: URL → discover files → enrich with LLM → load to PostgreSQL → query via MCP

## The V3 Bug We're Fixing

V3 had enrichment and loading as separate systems. Column names were generated in multiple places and drifted apart. Result: DDL had column X, INSERT had column Y, 0 rows loaded.

**The fix**: Enrichment returns a dict. Loader applies dict to `df.columns`. Both DDL and INSERT use `df.columns`. Single source of truth.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ BOOTSTRAP (one-time per publication)                                │
│                                                                     │
│  1. Discover files from URL                                         │
│  2. Group by period, pick latest                                    │
│  3. For each sheet:                                                 │
│     a. Detect grain (ICB/Trust/GP/National) from data values        │
│     b. Skip if grain=unknown (methodology, notes)                   │
│     c. Read sample rows                                             │
│     d. Call Claude API → get table name, column mappings,           │
│        column descriptions                                          │
│     e. Apply mappings to df.columns                                 │
│     f. Load using df.columns for DDL and INSERT                     │
│  4. Save PipelineConfig (includes mappings + descriptions + grain)  │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ SCAN (recurring, no LLM needed)                                     │
│                                                                     │
│  1. Load saved PipelineConfig                                       │
│  2. Discover current files                                          │
│  3. Find new periods                                                │
│  4. Load using saved mappings (same flow, skip enrichment)          │
│  5. If new column appears → flag for review or enrich just that col │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│ MCP (query time)                                                    │
│                                                                     │
│  1. list_datasets: read table names + descriptions + grain          │
│  2. get_schema: read column descriptions from config                │
│  3. query: execute SQL against staging tables                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Grain Detection (Entity Detection)

Before enrichment, detect the **grain** of each sheet by scanning data values:

```python
# src/datawarp/metadata/grain.py (under 80 lines)

ENTITY_PATTERNS = {
    'trust': {
        'pattern': r'^R[A-Z0-9]{1,4}$',
        'description': 'NHS Trust level',
        'examples': ['RJ1', 'RXH', 'R0A'],
        'priority': 100
    },
    'icb': {
        'pattern': r'^Q[A-Z0-9]{2}$',
        'description': 'Integrated Care Board level',
        'examples': ['QWE', 'QOP', 'QHG'],
        'priority': 100
    },
    'gp_practice': {
        'pattern': r'^[A-Z][0-9]{5}$',
        'description': 'GP Practice level',
        'examples': ['A81001', 'B82001'],
        'priority': 100
    },
    'region': {
        'pattern': r'^Y[0-9]{2}$',
        'description': 'NHS Region level',
        'examples': ['Y56', 'Y58', 'Y59'],
        'priority': 50
    },
    'national': {
        'pattern': None,  # Detected by keywords
        'keywords': ['ENGLAND', 'NATIONAL', 'TOTAL', 'ALL'],
        'description': 'National aggregate',
        'priority': 10
    }
}

def detect_grain(df: pd.DataFrame) -> dict:
    """
    Scan first few columns for entity codes.
    
    Returns:
        {
            "grain": "icb",  # or "trust", "gp_practice", "national", "unknown"
            "grain_column": "org_code",  # which column has the entity
            "confidence": 0.95,
            "description": "ICB level data"
        }
    """
    # Only check likely entity columns (short names, contains 'code', 'org', etc.)
    # Skip measure columns (contains 'count', 'total', 'rate', etc.)
    
    for col in df.columns[:10]:  # Check first 10 columns
        if is_measure_column(col):
            continue
        
        values = df[col].dropna().head(50).astype(str).str.upper().tolist()
        
        for entity_type, config in ENTITY_PATTERNS.items():
            if config.get('pattern'):
                matches = sum(1 for v in values if re.match(config['pattern'], v))
                confidence = matches / len(values) if values else 0
                
                if confidence >= 0.7:
                    return {
                        "grain": entity_type,
                        "grain_column": col,
                        "confidence": confidence,
                        "description": config['description']
                    }
            
            elif config.get('keywords'):
                if any(kw in ' '.join(values) for kw in config['keywords']):
                    return {
                        "grain": "national",
                        "grain_column": None,
                        "confidence": 0.8,
                        "description": "National aggregate data"
                    }
    
    return {"grain": "unknown", "grain_column": None, "confidence": 0, "description": ""}


def is_measure_column(col_name: str) -> bool:
    """Columns with these keywords are measures, not entities."""
    measures = ['count', 'total', 'number', 'percent', 'rate', 'ratio',
                'average', 'mean', 'median', 'sum', 'referrals', 'waiting']
    return any(m in col_name.lower() for m in measures)
```

**Usage in bootstrap:**
```python
# Before enrichment, detect grain
grain_info = detect_grain(df_sample)

if grain_info['grain'] == 'unknown' and grain_info['confidence'] == 0:
    # Skip this sheet - probably methodology or notes
    console.print(f"  [dim]Skipping {sheet_name} - no entity detected[/dim]")
    continue

# Pass grain to enrichment for better context
enriched = enrich_sheet(
    sheet_name=sheet_name,
    columns=sanitized_cols,
    sample_rows=sample_rows,
    publication_hint=pipeline_id,
    grain_hint=grain_info['grain']  # "icb", "trust", etc.
)
```

**Stored in SheetMapping:**
```python
sheet_mapping = SheetMapping(
    ...
    grain=grain_info['grain'],           # "icb"
    grain_column=grain_info['grain_column'],  # "org_code"  
    grain_description=grain_info['description']  # "ICB level data"
)
```

**MCP uses grain:**
```python
def list_datasets(conn):
    # Shows: "tbl_adhd_icb_referrals - ADHD referrals (ICB level)"
    return {
        "table": sm.table_name,
        "description": sm.table_description,
        "grain": sm.grain,  # Business users know this is ICB-level
        "grain_description": sm.grain_description
    }
```

---

## Data Structures

```python
@dataclass
class SheetMapping:
    sheet_pattern: str                        # "Table 2a" or regex
    table_name: str                           # "tbl_adhd_icb_referrals"
    table_description: str                    # "ADHD referrals by ICB"
    column_mappings: Dict[str, str]           # {"org_code": "icb_code"}
    column_descriptions: Dict[str, str]       # {"icb_code": "Integrated Care Board code"}
    column_types: Dict[str, str]              # {"icb_code": "TEXT"}
    grain: str                                # "icb", "trust", "national", "unknown"
    grain_column: Optional[str]               # "icb_code" - which column has entity
    grain_description: str                    # "ICB level data"

@dataclass
class FilePattern:
    filename_pattern: str                     # r"adhd.*\.xlsx"
    file_types: List[str]                     # ["xlsx"]
    sheet_mappings: List[SheetMapping]

@dataclass
class PipelineConfig:
    pipeline_id: str
    name: str
    landing_page: str
    file_patterns: List[FilePattern]
    loaded_periods: List[str]
    auto_load: bool
    # Stored as JSONB in datawarp.tbl_pipeline_configs
```

## Enrichment Function

**File**: `src/datawarp/metadata/enrich.py` (under 100 lines)

```python
import json
import anthropic

def enrich_sheet(
    sheet_name: str,
    columns: list[str],
    sample_rows: list[dict],
    publication_hint: str = ""
) -> dict:
    """
    Call Claude API to get semantic names and descriptions.
    
    Args:
        sheet_name: Original sheet name
        columns: List of column headers (already sanitized)
        sample_rows: First 3-5 rows as dicts
        publication_hint: e.g., "ADHD referrals", "MSA breaches"
    
    Returns:
        {
            "table_name": "adhd_icb_referrals",
            "table_description": "ADHD referrals by Integrated Care Board",
            "columns": {
                "org_code": "icb_code",
                "measure_1": "referrals_received"
            },
            "descriptions": {
                "icb_code": "Integrated Care Board identifier (e.g., QWE)",
                "referrals_received": "Number of ADHD referrals received in period"
            }
        }
    """
    client = anthropic.Anthropic()
    
    prompt = f"""You are analyzing an NHS dataset. Suggest semantic names for this data.

Sheet name: {sheet_name}
Publication context: {publication_hint}
Columns: {columns}
Sample data (first 3 rows):
{json.dumps(sample_rows[:3], indent=2, default=str)}

Respond with JSON only, no markdown:
{{
    "table_name": "lowercase_snake_case_descriptive_name",
    "table_description": "One sentence describing what this table contains",
    "columns": {{
        "original_col_name": "semantic_name",
        ...for each column
    }},
    "descriptions": {{
        "semantic_name": "What this column contains",
        ...for each column
    }}
}}

Rules:
- table_name: lowercase, snake_case, no prefix like tbl_
- Use NHS terminology: icb_code, trust_code, referrals, waiting_list
- Column names: lowercase, snake_case
- Descriptions: concise, mention units if applicable"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        # Parse JSON from response
        text = response.content[0].text
        # Handle potential markdown code blocks
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        
        return json.loads(text.strip())
        
    except Exception as e:
        # Fallback: return identity mappings
        print(f"Enrichment failed: {e}, using raw names")
        return {
            "table_name": sanitize_name(sheet_name),
            "table_description": f"Data from {sheet_name}",
            "columns": {c: c for c in columns},
            "descriptions": {c: "" for c in columns}
        }
```

## Loader Function (The Critical Part)

**File**: `src/datawarp/loader/excel.py`

```python
def load_sheet(
    file_path: Path,
    sheet_name: str,
    table_name: str,
    table_description: str,
    schema: str,
    period: str,
    column_mappings: Dict[str, str],      # from enrichment
    column_descriptions: Dict[str, str],   # from enrichment
    conn
) -> Tuple[bool, int, SheetMapping]:
    """
    Load Excel sheet to PostgreSQL.
    
    THE FIX: df.columns is the single source of truth.
    """
    
    # 1. Read data
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    if df.empty:
        return False, 0, None
    
    # 2. Sanitize original columns
    sanitized = {}
    for orig in df.columns:
        clean = sanitize_name(str(orig))
        if clean and not clean.startswith('unnamed'):
            sanitized[orig] = clean
    
    df = df[[c for c in df.columns if c in sanitized]]
    df.columns = [sanitized[c] for c in df.columns]
    
    # 3. Apply enrichment mappings
    # df.columns is now: ["org_code", "measure_1", ...]
    # column_mappings is: {"org_code": "icb_code", "measure_1": "referrals"}
    final_names = [column_mappings.get(c, c) for c in df.columns]
    df.columns = final_names
    
    # 4. NOW df.columns = ["icb_code", "referrals", ...] - SINGLE SOURCE OF TRUTH
    
    # 5. Infer types
    col_types = {}
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            col_types[col] = 'BIGINT'
        elif pd.api.types.is_float_dtype(df[col]):
            col_types[col] = 'DOUBLE PRECISION'
        else:
            col_types[col] = 'TEXT'
    
    # 6. Add metadata columns
    df['_period'] = period
    df['_source_file'] = file_path.name
    df['_loaded_at'] = datetime.now().isoformat()
    
    # 7. Create table using df.columns
    full_table = f"{schema}.{table_name}"
    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """, (schema, table_name.replace('tbl_', '')))
    existing = {r[0] for r in cur.fetchall()}
    
    if not existing:
        # Create table - DDL uses df.columns
        col_defs = ['_row_id SERIAL PRIMARY KEY']
        for col in df.columns:
            pg_type = col_types.get(col, 'TEXT')
            col_defs.append(f'"{col}" {pg_type}')
        
        cur.execute(f'CREATE TABLE {full_table} ({", ".join(col_defs)})')
        conn.commit()
    else:
        # Add missing columns
        for col in df.columns:
            if col not in existing:
                pg_type = col_types.get(col, 'TEXT')
                cur.execute(f'ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS "{col}" {pg_type}')
        conn.commit()
    
    # 8. Load data - COPY uses same df.columns
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep='\\N')
    buffer.seek(0)
    
    copy_cols = ', '.join(f'"{c}"' for c in df.columns)
    cur.copy_expert(f"COPY {full_table} ({copy_cols}) FROM STDIN WITH CSV NULL '\\N'", buffer)
    conn.commit()
    
    # 9. Return SheetMapping for saving to config
    result_mapping = SheetMapping(
        sheet_pattern=sheet_name,
        table_name=table_name,
        table_description=table_description,
        column_mappings=column_mappings,
        column_descriptions=column_descriptions,
        column_types=col_types
    )
    
    return True, len(df), result_mapping
```

## Bootstrap Flow

**File**: `scripts/pipeline.py`

```python
def run_bootstrap(url: str, pipeline_id: str, use_enrichment: bool = True):
    # 1. Discover
    files = scrape_landing_page(url)
    periods = group_by_period(files)
    latest = get_latest_period(periods)
    
    # 2. Select (interactive or auto)
    selected_files = select_files(periods[latest])
    
    # 3. Process each file
    file_patterns = []
    conn = get_connection()
    
    for file_info in selected_files:
        local_path = download_file(file_info.url, file_info.filename)
        sheet_mappings = []
        
        for sheet_name in get_sheet_names(local_path):
            # Read sample
            df_sample = pd.read_excel(local_path, sheet_name=sheet_name, nrows=10)
            sanitized_cols = [sanitize_name(c) for c in df_sample.columns]
            sample_rows = df_sample.head(3).to_dict('records')
            
            # Enrich
            if use_enrichment:
                enriched = enrich_sheet(
                    sheet_name=sheet_name,
                    columns=sanitized_cols,
                    sample_rows=sample_rows,
                    publication_hint=pipeline_id
                )
            else:
                enriched = {
                    "table_name": sanitize_name(sheet_name),
                    "table_description": "",
                    "columns": {c: c for c in sanitized_cols},
                    "descriptions": {}
                }
            
            # Load
            table_name = f"tbl_{pipeline_id}_{enriched['table_name']}"
            success, rows, sheet_mapping = load_sheet(
                file_path=local_path,
                sheet_name=sheet_name,
                table_name=table_name,
                table_description=enriched['table_description'],
                schema='staging',
                period=latest,
                column_mappings=enriched['columns'],
                column_descriptions=enriched['descriptions'],
                conn=conn
            )
            
            if success:
                sheet_mappings.append(sheet_mapping)
                record_load(pipeline_id, latest, table_name, file_info.filename, sheet_name, rows, conn)
        
        file_patterns.append(FilePattern(
            filename_pattern=make_pattern(file_info.filename),
            file_types=[file_info.file_type],
            sheet_mappings=sheet_mappings
        ))
    
    # 4. Save config
    config = PipelineConfig(
        pipeline_id=pipeline_id,
        name=pipeline_id.replace('_', ' ').title(),
        landing_page=url,
        file_patterns=file_patterns,
        loaded_periods=[latest],
        auto_load=True
    )
    save_pipeline(config, conn)
```

## Scan Flow

```python
def run_scan(pipeline_id: str):
    conn = get_connection()
    config = load_pipeline(pipeline_id, conn)
    
    # Discover current
    files = scrape_landing_page(config.landing_page)
    periods = group_by_period(files)
    
    # Find new
    loaded = set(get_loaded_periods(pipeline_id, conn))
    new_periods = [p for p in periods.keys() if p not in loaded]
    
    for period in new_periods:
        for file_info in periods[period]:
            # Match against saved patterns
            for file_pattern in config.file_patterns:
                if re.match(file_pattern.filename_pattern, file_info.filename):
                    local_path = download_file(file_info.url, file_info.filename)
                    
                    for sheet_mapping in file_pattern.sheet_mappings:
                        # USE SAVED MAPPINGS - no LLM call
                        success, rows, _ = load_sheet(
                            file_path=local_path,
                            sheet_name=sheet_mapping.sheet_pattern,
                            table_name=sheet_mapping.table_name,
                            table_description=sheet_mapping.table_description,
                            schema='staging',
                            period=period,
                            column_mappings=sheet_mapping.column_mappings,
                            column_descriptions=sheet_mapping.column_descriptions,
                            conn=conn
                        )
                        
                        if success:
                            record_load(pipeline_id, period, sheet_mapping.table_name, 
                                       file_info.filename, sheet_mapping.sheet_pattern, rows, conn)
```

## MCP Server

```python
def list_datasets(conn) -> List[dict]:
    """List all datasets with descriptions from saved configs."""
    configs = list_pipelines(conn)
    datasets = []
    
    for config in configs:
        for fp in config.file_patterns:
            for sm in fp.sheet_mappings:
                # Get row count from actual table
                cur = conn.cursor()
                cur.execute(f"SELECT COUNT(*) FROM staging.{sm.table_name}")
                row_count = cur.fetchone()[0]
                
                datasets.append({
                    "table_name": sm.table_name,
                    "description": sm.table_description,  # FROM CONFIG
                    "row_count": row_count,
                    "pipeline": config.pipeline_id
                })
    
    return datasets


def get_schema(table_name: str, conn) -> dict:
    """Get column descriptions from saved config."""
    # Find which config owns this table
    configs = list_pipelines(conn)
    
    for config in configs:
        for fp in config.file_patterns:
            for sm in fp.sheet_mappings:
                if sm.table_name == table_name:
                    # Get actual columns from DB
                    cur = conn.cursor()
                    cur.execute(f"""
                        SELECT column_name, data_type 
                        FROM information_schema.columns
                        WHERE table_schema = 'staging' AND table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))
                    
                    columns = []
                    for col_name, data_type in cur.fetchall():
                        if not col_name.startswith('_'):
                            columns.append({
                                "name": col_name,
                                "type": data_type,
                                "description": sm.column_descriptions.get(col_name, "")  # FROM CONFIG
                            })
                    
                    return {
                        "table_name": table_name,
                        "description": sm.table_description,
                        "columns": columns
                    }
    
    return None
```

## Database Schema

```sql
CREATE SCHEMA IF NOT EXISTS datawarp;
CREATE SCHEMA IF NOT EXISTS staging;

-- Just 2 tables for config
CREATE TABLE datawarp.tbl_pipeline_configs (
    pipeline_id VARCHAR(63) PRIMARY KEY,
    config JSONB NOT NULL,  -- Contains everything: mappings, descriptions
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

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

-- Data tables created dynamically in staging schema
```

## File Structure

```
datawarp-v3.1/
├── scripts/
│   ├── pipeline.py          # CLI: bootstrap, scan, backfill, list
│   └── mcp_server.py        # MCP tools
├── src/datawarp/
│   ├── discovery/
│   │   └── scraper.py       # URL scraping
│   ├── loader/
│   │   └── excel.py         # Load with df.columns fix
│   ├── metadata/
│   │   ├── enrich.py        # Claude API call (one function)
│   │   └── grain.py         # Entity/grain detection (one function)
│   ├── pipeline/
│   │   ├── config.py        # Dataclasses
│   │   └── repository.py    # Save/load configs
│   ├── storage/
│   │   └── connection.py    # DB connection
│   └── utils/
│       ├── period.py        # Parse YYYY-MM
│       └── sanitize.py      # Clean names
├── sql/
│   └── schema.sql
├── requirements.txt
└── README.md
```

## Requirements (requirements.txt)

```
pandas>=2.0
openpyxl>=3.1
psycopg2-binary>=2.9
requests>=2.31
beautifulsoup4>=4.12
rich>=13.0
python-dotenv>=1.0
anthropic>=0.18.0
```

## Rules - DO NOT VIOLATE

1. **df.columns is single source of truth** - DDL and INSERT both use it
2. **Enrichment returns dict** - does not write to database
3. **One JSONB blob stores everything** - mappings, descriptions, grain, types
4. **Scan uses saved mappings** - no LLM call on scan
5. **MCP reads from config** - not from separate metadata tables
6. **Grain detection filters sheets** - skip sheets with no detected entity
7. **Files under 300 lines**
8. **No factories, managers, orchestrators**

## Test Commands

```bash
# Bootstrap with enrichment
python scripts/pipeline.py bootstrap --url "https://digital.nhs.uk/.../mi-adhd" --enrich

# Verify semantic names
psql -c "SELECT table_name FROM information_schema.tables WHERE table_schema='staging'"
# Should show: tbl_mi_adhd_icb_referrals, tbl_mi_adhd_national_summary (not tbl_mi_adhd_table_2a)

# Verify grain detected
psql -c "SELECT config->'file_patterns'->0->'sheet_mappings'->0->>'grain' FROM datawarp.tbl_pipeline_configs"
# Should show: "icb" or "trust" or "national" (not "unknown")

# Verify descriptions saved
psql -c "SELECT config->'file_patterns'->0->'sheet_mappings'->0->'column_descriptions' FROM datawarp.tbl_pipeline_configs"

# Test MCP
python scripts/mcp_server.py --test
# Should show: table description + grain + column descriptions
```
