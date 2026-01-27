# DataWarp MVP - Complete Repository Structure

## Directory Layout

```
datawarp-mvp/
├── README.md                    # Quick start guide
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment template
│
├── scripts/
│   ├── pipeline.py              # Main CLI: bootstrap, scan, backfill, list
│   └── mcp_server.py            # MCP server for Claude integration
│
├── src/
│   └── datawarp/
│       ├── __init__.py
│       │
│       ├── discovery/           # URL scraping (extracted from existing khoj)
│       │   ├── __init__.py
│       │   ├── scraper.py       # Landing page scraper
│       │   └── html_parser.py   # Link extraction
│       │
│       ├── loader/              # Data loading (new, simplified)
│       │   ├── __init__.py
│       │   ├── download.py      # File download with caching
│       │   ├── extract.py       # Excel/CSV reading
│       │   └── load.py          # PostgreSQL loading
│       │
│       ├── metadata/            # Heuristic metadata (new)
│       │   ├── __init__.py
│       │   ├── patterns.py      # NHS entity patterns, column patterns
│       │   └── inference.py     # Description generation
│       │
│       ├── storage/             # Database (simplified from existing)
│       │   ├── __init__.py
│       │   ├── connection.py    # PostgreSQL connection
│       │   └── schema.sql       # Minimal schema (2 tables)
│       │
│       └── utils/               # Utilities (extracted from existing)
│           ├── __init__.py
│           ├── period.py        # Period parsing
│           └── sanitize.py      # Name sanitization
│
├── config/
│   └── mcp_config.json          # MCP server configuration for Claude Desktop
│
├── downloads/                   # File cache (gitignored)
│
└── tests/
    ├── test_discovery.py
    ├── test_loader.py
    └── test_mcp.py
```

## File Count Summary

| Category | Files | Purpose |
|----------|-------|---------|
| Scripts | 2 | CLI entry points |
| Discovery | 3 | URL scraping |
| Loader | 4 | Data loading |
| Metadata | 3 | Column descriptions |
| Storage | 3 | Database |
| Utils | 3 | Helpers |
| Config | 2 | Settings |
| Tests | 3 | Verification |
| **Total** | **~23 files** | vs 100+ in original |

## Core Files Content

### `requirements.txt`
```
pandas>=2.0
psycopg2-binary>=2.9
openpyxl>=3.1
requests>=2.28
beautifulsoup4>=4.12
rich>=13.0
python-dotenv>=1.0
```

### `.env.example`
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=datawarp
DB_USER=postgres
DB_PASSWORD=
DOWNLOAD_DIR=./downloads
```

### `src/datawarp/storage/schema.sql`
```sql
-- Run once to initialize database
CREATE SCHEMA IF NOT EXISTS datawarp;
CREATE SCHEMA IF NOT EXISTS staging;

-- Pipeline configurations (JSONB for flexibility)
CREATE TABLE IF NOT EXISTS datawarp.tbl_pipeline_configs (
    pipeline_id VARCHAR(63) PRIMARY KEY,
    config JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Load history for tracking
CREATE TABLE IF NOT EXISTS datawarp.tbl_load_history (
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

-- Index for fast period lookups
CREATE INDEX IF NOT EXISTS idx_load_history_pipeline_period 
ON datawarp.tbl_load_history(pipeline_id, period);
```

### `src/datawarp/__init__.py`
```python
"""DataWarp MVP - NHS Data Pipeline."""
__version__ = "0.1.0"
```

### `src/datawarp/storage/connection.py`
```python
"""Database connection management."""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Get PostgreSQL connection from environment."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME', 'datawarp'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', '')
    )
```

### `src/datawarp/utils/sanitize.py`
```python
"""Name sanitization utilities."""
import re

def sanitize_name(name: str, max_len: int = 63) -> str:
    """Create PostgreSQL-safe identifier."""
    if not name:
        return 'unnamed'
    
    clean = re.sub(r'[^a-z0-9]+', '_', str(name).lower())
    clean = re.sub(r'^_|_$', '', clean)
    clean = re.sub(r'_+', '_', clean)
    
    if clean and clean[0].isdigit():
        clean = 'c_' + clean
    
    return clean[:max_len] if clean else 'unnamed'


def sanitize_table_name(name: str) -> str:
    """Create table name from filename/sheet."""
    return f"tbl_{sanitize_name(name)}"
```

### `src/datawarp/utils/period.py`
```python
"""Period parsing from filenames and URLs."""
import re
from typing import Optional, Tuple

MONTH_MAP = {
    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
    'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
    'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
}

def parse_period(text: str) -> Optional[str]:
    """Extract YYYY-MM period from text."""
    if not text:
        return None
    
    text_lower = text.lower()
    
    # Try YYYY-MM format
    match = re.search(r'(20\d{2})[-_]?(0[1-9]|1[0-2])', text)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    
    # Try Month-YYYY format
    for month, num in MONTH_MAP.items():
        match = re.search(rf'{month}[a-z]*[-_]?(20\d{{2}})', text_lower)
        if match:
            return f"{match.group(1)}-{num}"
        match = re.search(rf'(20\d{{2}})[-_]?{month}', text_lower)
        if match:
            return f"{match.group(1)}-{num}"
    
    return None
```

### `src/datawarp/metadata/patterns.py`
```python
"""NHS entity and column patterns for heuristic metadata."""

# NHS organization code patterns
NHS_ENTITIES = {
    'icb': {
        'pattern': r'^Q[A-Z]{2}$',
        'description': 'Integrated Care Board (ICB) code',
        'hints': ['icb_code', 'icb', 'commissioner_code']
    },
    'trust': {
        'pattern': r'^R[A-Z0-9]{2,4}$',
        'description': 'NHS Trust code',
        'hints': ['trust_code', 'provider_code', 'org_code']
    },
    'gp_practice': {
        'pattern': r'^[A-Z]\d{5}$',
        'description': 'GP Practice code',
        'hints': ['practice_code', 'gp_code']
    },
}

# Column name patterns → description templates
COLUMN_PATTERNS = {
    r'.*_count$': 'Count of {subject}',
    r'.*_total$': 'Total {subject}',
    r'.*_rate$': 'Rate per population',
    r'.*_pct$|.*_percent': 'Percentage',
    r'.*_date$': 'Date value',
    r'.*_code$': 'Identifier code',
    r'.*_name$': 'Display name',
    r'.*referral.*': 'Referral metric',
    r'.*waiting.*': 'Waiting time/list metric',
}

# Known canonical column definitions
KNOWN_COLUMNS = {
    'icb_code': 'Integrated Care Board identifier (e.g., QWE)',
    'icb_name': 'Integrated Care Board name',
    'trust_code': 'NHS Trust identifier (e.g., RJ1)',
    'provider_name': 'Healthcare provider name',
    'period': 'Reporting period (YYYY-MM)',
    'referral_count': 'Number of referrals received',
    'waiting_list_size': 'Patients currently on waiting list',
}
```

### `config/mcp_config.json`
```json
{
  "mcpServers": {
    "datawarp": {
      "command": "python",
      "args": ["scripts/mcp_server.py"],
      "env": {
        "DB_HOST": "localhost",
        "DB_NAME": "datawarp"
      }
    }
  }
}
```

### `README.md`
```markdown
# DataWarp MVP

NHS data pipeline with MCP integration for Claude.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure database
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 3. Initialize schema
psql -f src/datawarp/storage/schema.sql

# 4. Bootstrap a publication
python scripts/pipeline.py bootstrap \
    --url "https://digital.nhs.uk/data-and-information/publications/statistical/mi-adhd"

# 5. Check for new data (run periodically)
python scripts/pipeline.py scan --pipeline adhd

# 6. Test MCP server
python scripts/mcp_server.py --test
```

## Commands

| Command | Purpose |
|---------|---------|
| `pipeline.py bootstrap --url <url>` | Create new pipeline interactively |
| `pipeline.py scan --pipeline <id>` | Load new periods |
| `pipeline.py backfill --pipeline <id>` | Load all history |
| `pipeline.py list` | Show registered pipelines |
| `mcp_server.py --test` | Test MCP tools locally |

## Claude Desktop Integration

Add to Claude Desktop config:
```json
{
  "mcpServers": {
    "datawarp": {
      "command": "python",
      "args": ["/path/to/datawarp-mvp/scripts/mcp_server.py"]
    }
  }
}
```
```

## What Gets Extracted from Existing Codebase

| Existing File | MVP Equivalent | Changes |
|---------------|----------------|---------|
| `khoj/scraper.py` | `discovery/scraper.py` | Simplified, removed DB writes |
| `discovery/html_parser.py` | `discovery/html_parser.py` | As-is |
| `utils/period.py` | `utils/period.py` | Simplified |
| `storage/connection.py` | `storage/connection.py` | As-is |
| `core/extractor.py` | `loader/extract.py` | Sheet analysis only |

## What Gets Removed

- Agent state machine (500 lines)
- Queue system (400 lines)
- 39-table schema → 2 tables
- Models layer (800 lines) → JSONB config
- Registry/drift detection (600 lines)
- Orchestrator (400 lines)

## Line Count Comparison

| Component | Original | MVP |
|-----------|----------|-----|
| Discovery | 1,200 | 300 |
| Loading | 2,500 | 400 |
| Storage | 1,500 | 100 |
| Agent/CLI | 2,000 | 500 |
| Models | 800 | 0 (JSONB) |
| Queue | 400 | 0 |
| **Total** | **~27,000** | **~2,000** |
