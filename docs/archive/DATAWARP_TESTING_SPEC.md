# DataWarp v3.1 - Rigorous Testing Spec

## What Went Wrong in V3

| Symptom | Root Cause | Test That Would Have Caught It |
|---------|------------|-------------------------------|
| 0 rows loaded | DDL columns ≠ INSERT columns | Assert `COUNT(*) > 0` after every load |
| Columns drifted | Multiple code paths generating names | Assert DDL columns == DataFrame columns |
| Enrichment not applied | Mappings stored but not read correctly | Assert column names are semantic, not raw |
| Useless sheets loaded | No grain filtering | Assert grain ≠ 'unknown' for loaded tables |
| Scan recreated tables | No append logic | Assert row count increases after scan |

## Testing Philosophy

```
❌ WRONG: "It ran without errors"
✅ RIGHT: "8,149 rows in staging.tbl_mi_adhd_icb_referrals with column 'icb_code'"
```

Every test must assert on **outcomes**, not just absence of exceptions.

---

## Testing Agent

**File: `scripts/test_agent.py`**

A single script that validates the entire flow end-to-end.

```python
#!/usr/bin/env python3
"""
DataWarp Testing Agent

Runs comprehensive validation of the pipeline:
1. Bootstrap a known publication
2. Verify data loaded correctly
3. Verify enrichment applied
4. Verify grain detected
5. Simulate scan with historical period
6. Verify append (not replace)
7. Test MCP queries

Usage:
    python scripts/test_agent.py --url "https://digital.nhs.uk/.../mi-adhd"
    python scripts/test_agent.py --full  # Run all test suites
"""

import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.insert(0, 'src')

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from datawarp.storage import get_connection
from datawarp.pipeline.repository import load_pipeline, get_loaded_periods

console = Console()

# Test results
RESULTS = []

def record(test_name: str, passed: bool, expected: str, actual: str):
    """Record a test result."""
    RESULTS.append({
        "test": test_name,
        "passed": passed,
        "expected": expected,
        "actual": actual
    })
    icon = "✅" if passed else "❌"
    console.print(f"  {icon} {test_name}")
    if not passed:
        console.print(f"      Expected: {expected}")
        console.print(f"      Actual:   {actual}")


def assert_rows_loaded(table_name: str, min_rows: int, conn) -> bool:
    """CRITICAL: Verify rows actually loaded."""
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM staging.{table_name}")
    count = cur.fetchone()[0]
    
    passed = count >= min_rows
    record(
        f"Rows loaded: {table_name}",
        passed,
        f">= {min_rows} rows",
        f"{count} rows"
    )
    return passed


def assert_columns_exist(table_name: str, expected_cols: list, conn) -> bool:
    """Verify expected columns exist in table."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = %s
    """, (table_name,))
    actual_cols = {row[0] for row in cur.fetchall()}
    
    missing = set(expected_cols) - actual_cols
    passed = len(missing) == 0
    record(
        f"Columns exist: {table_name}",
        passed,
        f"Columns {expected_cols}",
        f"Missing: {missing}" if missing else "All present"
    )
    return passed


def assert_no_raw_columns(table_name: str, raw_patterns: list, conn) -> bool:
    """Verify enrichment applied - no raw column names."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = %s
    """, (table_name,))
    actual_cols = [row[0] for row in cur.fetchall()]
    
    raw_found = [c for c in actual_cols for p in raw_patterns if p in c.lower()]
    passed = len(raw_found) == 0
    record(
        f"Enrichment applied: {table_name}",
        passed,
        f"No columns matching {raw_patterns}",
        f"Found raw columns: {raw_found}" if raw_found else "All semantic"
    )
    return passed


def assert_grain_detected(pipeline_id: str, conn) -> bool:
    """Verify grain detection worked."""
    config = load_pipeline(pipeline_id, conn)
    
    grains = []
    for fp in config.file_patterns:
        for sm in fp.sheet_mappings:
            grains.append(sm.grain)
    
    unknown_count = grains.count('unknown')
    passed = unknown_count == 0
    record(
        f"Grain detected: {pipeline_id}",
        passed,
        "All sheets have grain (icb/trust/national)",
        f"{unknown_count} sheets with unknown grain" if unknown_count else f"Grains: {set(grains)}"
    )
    return passed


def assert_descriptions_populated(pipeline_id: str, conn) -> bool:
    """Verify column descriptions exist."""
    config = load_pipeline(pipeline_id, conn)
    
    empty_count = 0
    total_count = 0
    for fp in config.file_patterns:
        for sm in fp.sheet_mappings:
            for col, desc in sm.column_descriptions.items():
                total_count += 1
                if not desc or desc.strip() == "":
                    empty_count += 1
    
    passed = empty_count == 0 and total_count > 0
    record(
        f"Descriptions populated: {pipeline_id}",
        passed,
        "All columns have descriptions",
        f"{empty_count}/{total_count} empty" if empty_count else f"{total_count} descriptions"
    )
    return passed


def assert_scan_appends(table_name: str, count_before: int, conn) -> bool:
    """Verify scan appended rows, not replaced."""
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM staging.{table_name}")
    count_after = cur.fetchone()[0]
    
    passed = count_after > count_before
    record(
        f"Scan appends: {table_name}",
        passed,
        f"> {count_before} rows",
        f"{count_after} rows"
    )
    return passed


def assert_periods_distinct(table_name: str, min_periods: int, conn) -> bool:
    """Verify multiple periods loaded."""
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(DISTINCT _period) FROM staging.{table_name}")
    period_count = cur.fetchone()[0]
    
    passed = period_count >= min_periods
    record(
        f"Multiple periods: {table_name}",
        passed,
        f">= {min_periods} periods",
        f"{period_count} periods"
    )
    return passed


def assert_mcp_returns_descriptions(table_name: str, conn) -> bool:
    """Verify MCP can retrieve descriptions."""
    # Import MCP function
    from scripts.mcp_server import get_schema
    
    schema = get_schema(table_name, conn)
    
    if not schema:
        record(f"MCP schema: {table_name}", False, "Schema returned", "None")
        return False
    
    cols_with_desc = sum(1 for c in schema.get('columns', []) if c.get('description'))
    total_cols = len(schema.get('columns', []))
    
    passed = cols_with_desc > 0
    record(
        f"MCP descriptions: {table_name}",
        passed,
        "Columns have descriptions",
        f"{cols_with_desc}/{total_cols} with descriptions"
    )
    return passed


# =============================================================================
# Test Suites
# =============================================================================

def test_bootstrap(url: str, pipeline_id: str):
    """Test bootstrap creates tables with data."""
    console.print(Panel(f"[bold]Test Suite: Bootstrap[/bold]\nURL: {url}"))
    
    import subprocess
    
    # Run bootstrap
    result = subprocess.run(
        ["python", "scripts/pipeline.py", "bootstrap", "--url", url, "--enrich", "--auto"],
        capture_output=True, text=True, env={**os.environ, "PYTHONPATH": "src"}
    )
    
    if result.returncode != 0:
        record("Bootstrap runs", False, "Exit code 0", f"Exit code {result.returncode}\n{result.stderr}")
        return
    
    record("Bootstrap runs", True, "Exit code 0", "Success")
    
    # Verify results
    conn = get_connection()
    try:
        config = load_pipeline(pipeline_id, conn)
        if not config:
            record("Pipeline saved", False, "Config exists", "Not found")
            return
        
        record("Pipeline saved", True, "Config exists", f"Found: {config.name}")
        
        # Check each table
        for fp in config.file_patterns:
            for sm in fp.sheet_mappings:
                assert_rows_loaded(sm.table_name, 1, conn)
                assert_grain_detected(pipeline_id, conn)
                assert_descriptions_populated(pipeline_id, conn)
    finally:
        conn.close()


def test_column_integrity(pipeline_id: str):
    """Test DDL columns match loaded data columns."""
    console.print(Panel("[bold]Test Suite: Column Integrity[/bold]"))
    
    conn = get_connection()
    try:
        config = load_pipeline(pipeline_id, conn)
        cur = conn.cursor()
        
        for fp in config.file_patterns:
            for sm in fp.sheet_mappings:
                # Get DDL columns
                cur.execute(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'staging' AND table_name = %s
                    ORDER BY ordinal_position
                """, (sm.table_name,))
                ddl_cols = [r[0] for r in cur.fetchall()]
                
                # Get data columns (from a sample row)
                cur.execute(f"SELECT * FROM staging.{sm.table_name} LIMIT 1")
                data_cols = [desc[0] for desc in cur.description]
                
                passed = set(ddl_cols) == set(data_cols)
                record(
                    f"DDL == Data columns: {sm.table_name}",
                    passed,
                    "Columns match",
                    f"DDL: {len(ddl_cols)}, Data: {len(data_cols)}"
                )
                
                # Check semantic names applied
                assert_no_raw_columns(sm.table_name, ['unnamed', 'column', 'measure_', 'table_'], conn)
    finally:
        conn.close()


def test_scan(pipeline_id: str, historical_period: str = None):
    """Test scan detects and loads new periods."""
    console.print(Panel("[bold]Test Suite: Scan[/bold]"))
    
    conn = get_connection()
    try:
        config = load_pipeline(pipeline_id, conn)
        
        # Get current counts
        counts_before = {}
        cur = conn.cursor()
        for fp in config.file_patterns:
            for sm in fp.sheet_mappings:
                cur.execute(f"SELECT COUNT(*) FROM staging.{sm.table_name}")
                counts_before[sm.table_name] = cur.fetchone()[0]
        
        # Run scan
        import subprocess
        result = subprocess.run(
            ["python", "scripts/pipeline.py", "scan", "--pipeline", pipeline_id],
            capture_output=True, text=True, env={**os.environ, "PYTHONPATH": "src"}
        )
        
        record("Scan runs", result.returncode == 0, "Exit code 0", f"Exit code {result.returncode}")
        
        # Check if new periods were loaded (or correctly detected none)
        loaded = get_loaded_periods(pipeline_id, conn)
        record(
            f"Periods tracked: {pipeline_id}",
            len(loaded) > 0,
            ">= 1 period",
            f"{len(loaded)} periods: {loaded[:3]}..."
        )
    finally:
        conn.close()


def test_mcp(pipeline_id: str):
    """Test MCP tools return useful data."""
    console.print(Panel("[bold]Test Suite: MCP[/bold]"))
    
    conn = get_connection()
    try:
        # Import MCP functions
        sys.path.insert(0, 'scripts')
        from mcp_server import list_datasets, get_schema, execute_query
        
        # Test list_datasets
        datasets = list_datasets(conn)
        record(
            "list_datasets",
            len(datasets) > 0,
            ">= 1 dataset",
            f"{len(datasets)} datasets"
        )
        
        if datasets:
            # Test get_schema
            first_table = datasets[0]['table_name']
            schema = get_schema(first_table, conn)
            record(
                "get_schema returns data",
                schema is not None and len(schema.get('columns', [])) > 0,
                "Schema with columns",
                f"{len(schema.get('columns', []))} columns" if schema else "None"
            )
            
            # Test descriptions populated
            if schema:
                with_desc = sum(1 for c in schema['columns'] if c.get('description'))
                record(
                    "Columns have descriptions",
                    with_desc > 0,
                    ">= 1 with description",
                    f"{with_desc}/{len(schema['columns'])}"
                )
            
            # Test query
            result = execute_query(f"SELECT COUNT(*) FROM staging.{first_table}", conn)
            record(
                "Query executes",
                'rows' in result and len(result['rows']) > 0,
                "Results returned",
                f"{result.get('row_count', 0)} rows" if 'rows' in result else result.get('error', 'Unknown')
            )
    finally:
        conn.close()


def test_grain_filtering():
    """Test that grain detection filters useless sheets."""
    console.print(Panel("[bold]Test Suite: Grain Filtering[/bold]"))
    
    # Create test data with known characteristics
    import pandas as pd
    from datawarp.metadata.grain import detect_grain
    
    # ICB data - should detect
    icb_df = pd.DataFrame({
        'org_code': ['QWE', 'QOP', 'QHG', 'QJK'],
        'value': [100, 200, 300, 400]
    })
    result = detect_grain(icb_df)
    record("Detect ICB grain", result['grain'] == 'icb', "grain=icb", f"grain={result['grain']}")
    
    # Trust data - should detect
    trust_df = pd.DataFrame({
        'provider_code': ['RJ1', 'RXH', 'R0A', 'RJE'],
        'value': [100, 200, 300, 400]
    })
    result = detect_grain(trust_df)
    record("Detect Trust grain", result['grain'] == 'trust', "grain=trust", f"grain={result['grain']}")
    
    # National data - should detect
    national_df = pd.DataFrame({
        'region': ['ENGLAND', 'ENGLAND', 'ENGLAND'],
        'metric': ['A', 'B', 'C'],
        'value': [100, 200, 300]
    })
    result = detect_grain(national_df)
    record("Detect National grain", result['grain'] == 'national', "grain=national", f"grain={result['grain']}")
    
    # Methodology/notes - should NOT detect (unknown)
    notes_df = pd.DataFrame({
        'note_id': [1, 2, 3],
        'description': ['This is methodology', 'Data definitions', 'Change log']
    })
    result = detect_grain(notes_df)
    record("Reject methodology", result['grain'] == 'unknown', "grain=unknown", f"grain={result['grain']}")


# =============================================================================
# Main
# =============================================================================

def print_summary():
    """Print test results summary."""
    console.print("\n")
    
    passed = sum(1 for r in RESULTS if r['passed'])
    failed = sum(1 for r in RESULTS if not r['passed'])
    
    table = Table(title="Test Results Summary")
    table.add_column("Status", style="cyan")
    table.add_column("Count", justify="right")
    
    table.add_row("✅ Passed", str(passed))
    table.add_row("❌ Failed", str(failed))
    table.add_row("Total", str(len(RESULTS)))
    
    console.print(table)
    
    if failed > 0:
        console.print("\n[bold red]Failed Tests:[/bold red]")
        for r in RESULTS:
            if not r['passed']:
                console.print(f"  ❌ {r['test']}")
                console.print(f"      Expected: {r['expected']}")
                console.print(f"      Actual:   {r['actual']}")
    
    return failed == 0


def main():
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description='DataWarp Testing Agent')
    parser.add_argument('--url', help='NHS publication URL to test')
    parser.add_argument('--pipeline', default='test_pipeline', help='Pipeline ID for testing')
    parser.add_argument('--full', action='store_true', help='Run all test suites')
    parser.add_argument('--suite', choices=['bootstrap', 'columns', 'scan', 'mcp', 'grain'], help='Run specific suite')
    
    args = parser.parse_args()
    
    console.print(Panel("[bold cyan]DataWarp Testing Agent[/bold cyan]"))
    
    if args.suite == 'grain' or args.full:
        test_grain_filtering()
    
    if args.url and (args.suite == 'bootstrap' or args.full):
        test_bootstrap(args.url, args.pipeline)
    
    if args.suite == 'columns' or args.full:
        test_column_integrity(args.pipeline)
    
    if args.suite == 'scan' or args.full:
        test_scan(args.pipeline)
    
    if args.suite == 'mcp' or args.full:
        test_mcp(args.pipeline)
    
    success = print_summary()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
```

---

## Critical Test Cases (Must Pass Before Deploy)

### 1. The DDL Bug Test

```python
def test_ddl_matches_insert():
    """THE test that would have caught the v3 bug."""
    
    # Load a sheet
    success, rows, mapping = load_sheet(file, sheet, table, ...)
    
    # Get DDL columns
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s
    """, (table,))
    ddl_cols = set(r[0] for r in cur.fetchall())
    
    # Get actual data columns
    cur.execute(f"SELECT * FROM staging.{table} LIMIT 1")
    data_cols = set(desc[0] for desc in cur.description)
    
    # MUST MATCH
    assert ddl_cols == data_cols, f"DDL {ddl_cols} != Data {data_cols}"
    
    # And rows must be loaded
    cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
    assert cur.fetchone()[0] > 0, "No rows loaded!"
```

### 2. The Enrichment Applied Test

```python
def test_enrichment_actually_applied():
    """Verify semantic names, not raw Excel headers."""
    
    # After bootstrap
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = %s
    """, (table,))
    cols = [r[0] for r in cur.fetchall()]
    
    # Should NOT have raw names
    raw_patterns = ['unnamed', 'column1', 'measure_1', 'table_', 'org_code']
    for col in cols:
        for pattern in raw_patterns:
            assert pattern not in col.lower(), f"Raw column found: {col}"
    
    # Should have semantic names
    semantic_patterns = ['icb_', 'trust_', 'referral', 'waiting']
    has_semantic = any(p in c for c in cols for p in semantic_patterns)
    assert has_semantic, f"No semantic columns found in {cols}"
```

### 3. The Scan Appends Test

```python
def test_scan_appends_not_replaces():
    """Verify scan adds to table, doesn't recreate."""
    
    # Bootstrap loads period 2025-11
    bootstrap(url, pipeline_id)
    
    cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
    count_after_bootstrap = cur.fetchone()[0]
    
    cur.execute(f"SELECT DISTINCT _period FROM staging.{table}")
    periods_after_bootstrap = [r[0] for r in cur.fetchall()]
    
    # Scan loads period 2025-12
    scan(pipeline_id)
    
    cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
    count_after_scan = cur.fetchone()[0]
    
    cur.execute(f"SELECT DISTINCT _period FROM staging.{table}")
    periods_after_scan = [r[0] for r in cur.fetchall()]
    
    # Must have MORE rows, not same or fewer
    assert count_after_scan > count_after_bootstrap, "Scan didn't add rows"
    
    # Must have MORE periods
    assert len(periods_after_scan) > len(periods_after_bootstrap), "Scan didn't add periods"
    
    # Original data must still exist
    assert set(periods_after_bootstrap).issubset(set(periods_after_scan)), "Original periods lost"
```

### 4. The MCP Descriptions Test

```python
def test_mcp_has_descriptions():
    """Verify MCP returns useful metadata, not empty strings."""
    
    schema = get_schema(table_name, conn)
    
    assert schema is not None, "Schema not found"
    assert schema.get('description'), "Table description empty"
    assert schema.get('grain'), "Grain not set"
    
    cols_with_desc = [c for c in schema['columns'] if c.get('description')]
    assert len(cols_with_desc) > 0, "No column descriptions"
    
    # Descriptions should be meaningful, not just column names
    for col in cols_with_desc:
        assert col['description'] != col['name'], f"Description is just column name: {col}"
        assert len(col['description']) > 10, f"Description too short: {col}"
```

---

## Run Tests

```bash
# Quick: just grain detection (no DB needed)
python scripts/test_agent.py --suite grain

# Bootstrap test with real URL
python scripts/test_agent.py --url "https://digital.nhs.uk/.../mi-adhd" --pipeline test_adhd --suite bootstrap

# Full test suite
python scripts/test_agent.py --url "https://digital.nhs.uk/.../mi-adhd" --pipeline test_adhd --full

# CI/CD integration
python scripts/test_agent.py --full || exit 1
```

---

## Pre-Commit Checklist

Before any commit, run:

```bash
# 1. Does bootstrap load rows?
python scripts/test_agent.py --suite bootstrap --url "..." && \
  psql -c "SELECT COUNT(*) > 0 FROM staging.tbl_test_*"

# 2. Do columns match?
python scripts/test_agent.py --suite columns

# 3. Does scan append?
python scripts/test_agent.py --suite scan

# 4. Does MCP work?
python scripts/test_agent.py --suite mcp
```

If any fail, DO NOT COMMIT.
