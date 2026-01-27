#!/usr/bin/env python3
"""
DataWarp Pipeline System - Bootstrap to Auto-load Bridge

The core insight: Bootstrap PRODUCES configuration that Auto-load CONSUMES.

Pipeline lifecycle:
1. BOOTSTRAP: User discovers → selects → loads → system learns pattern
2. REGISTER: Pattern saved as pipeline config (which sheets, which tables, column mappings)
3. AUTO-LOAD: Future scans find new periods → load using saved pattern

Usage:
    # Bootstrap new publication (interactive)
    python scripts/pipeline.py bootstrap --url "https://digital.nhs.uk/.../mi-adhd"
    
    # Check for new data and auto-load
    python scripts/pipeline.py scan --pipeline adhd
    
    # List registered pipelines
    python scripts/pipeline.py list
"""

import sys
import json
import argparse
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from io import StringIO

sys.path.insert(0, 'src')

import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

from datawarp.storage.connection import get_connection
from datawarp.khoj.scraper import scrape_landing_page
from datawarp.khoj import download_file_to_path
from datawarp.core.extractor import FileExtractor
from datawarp.utils.period import parse_period

console = Console()
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

STAGING_SCHEMA = 'staging'
PIPELINES_SCHEMA = 'datawarp'
DOWNLOAD_DIR = Path('./downloads')


# =============================================================================
# Pipeline Configuration Schema
# =============================================================================

@dataclass
class SheetMapping:
    """Maps a source sheet pattern to a target table."""
    sheet_pattern: str          # Regex or exact match for sheet name
    table_name: str             # Target table in staging schema
    column_mappings: Dict[str, str] = field(default_factory=dict)  # source_col -> canonical_name
    column_types: Dict[str, str] = field(default_factory=dict)     # canonical_name -> pg_type
    is_primary: bool = True     # Primary data vs supporting


@dataclass
class FilePattern:
    """Identifies which files to process from a publication."""
    filename_pattern: str       # Regex for matching filenames
    file_types: List[str]       # ['xlsx', 'csv']
    sheet_mappings: List[SheetMapping] = field(default_factory=list)
    category: str = 'data'      # 'data', 'supporting', 'methodology'


@dataclass
class PipelineConfig:
    """Complete configuration for auto-loading a publication."""
    pipeline_id: str
    name: str
    landing_page: str
    file_patterns: List[FilePattern] = field(default_factory=list)
    loaded_periods: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_scan_at: Optional[str] = None
    auto_load: bool = False     # Enable fully automatic loading
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'PipelineConfig':
        file_patterns = [
            FilePattern(
                filename_pattern=fp['filename_pattern'],
                file_types=fp['file_types'],
                sheet_mappings=[SheetMapping(**sm) for sm in fp.get('sheet_mappings', [])],
                category=fp.get('category', 'data')
            )
            for fp in data.get('file_patterns', [])
        ]
        return cls(
            pipeline_id=data['pipeline_id'],
            name=data['name'],
            landing_page=data['landing_page'],
            file_patterns=file_patterns,
            loaded_periods=data.get('loaded_periods', []),
            created_at=data.get('created_at', datetime.now().isoformat()),
            last_scan_at=data.get('last_scan_at'),
            auto_load=data.get('auto_load', False)
        )


# =============================================================================
# Database Operations
# =============================================================================

def ensure_pipeline_schema(conn):
    """Create pipeline management tables."""
    cur = conn.cursor()
    
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {PIPELINES_SCHEMA}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")
    
    # Simple pipeline config storage (JSONB for flexibility)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {PIPELINES_SCHEMA}.tbl_pipeline_configs (
            pipeline_id VARCHAR(63) PRIMARY KEY,
            config JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    # Load history for tracking what's been loaded
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {PIPELINES_SCHEMA}.tbl_load_history (
            id SERIAL PRIMARY KEY,
            pipeline_id VARCHAR(63) REFERENCES {PIPELINES_SCHEMA}.tbl_pipeline_configs(pipeline_id),
            period VARCHAR(20) NOT NULL,
            table_name VARCHAR(63) NOT NULL,
            source_file TEXT,
            sheet_name VARCHAR(100),
            rows_loaded INT,
            loaded_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(pipeline_id, period, table_name, sheet_name)
        )
    """)
    
    conn.commit()


def save_pipeline(config: PipelineConfig, conn):
    """Save or update pipeline configuration."""
    cur = conn.cursor()
    
    cur.execute(f"""
        INSERT INTO {PIPELINES_SCHEMA}.tbl_pipeline_configs (pipeline_id, config)
        VALUES (%s, %s)
        ON CONFLICT (pipeline_id) DO UPDATE
        SET config = EXCLUDED.config, updated_at = NOW()
    """, (config.pipeline_id, json.dumps(config.to_dict())))
    
    conn.commit()
    console.print(f"[green]✓ Pipeline '{config.pipeline_id}' saved[/green]")


def load_pipeline(pipeline_id: str, conn) -> Optional[PipelineConfig]:
    """Load pipeline configuration."""
    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT config FROM {PIPELINES_SCHEMA}.tbl_pipeline_configs
        WHERE pipeline_id = %s
    """, (pipeline_id,))
    
    row = cur.fetchone()
    if not row:
        return None
    
    return PipelineConfig.from_dict(row[0])


def list_pipelines(conn) -> List[PipelineConfig]:
    """List all registered pipelines."""
    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT config FROM {PIPELINES_SCHEMA}.tbl_pipeline_configs
        ORDER BY created_at DESC
    """)
    
    return [PipelineConfig.from_dict(row[0]) for row in cur.fetchall()]


def record_load(pipeline_id: str, period: str, table_name: str, 
                source_file: str, sheet_name: str, rows: int, conn):
    """Record a successful load."""
    cur = conn.cursor()
    
    cur.execute(f"""
        INSERT INTO {PIPELINES_SCHEMA}.tbl_load_history 
        (pipeline_id, period, table_name, source_file, sheet_name, rows_loaded)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (pipeline_id, period, table_name, sheet_name) DO UPDATE
        SET rows_loaded = EXCLUDED.rows_loaded, loaded_at = NOW()
    """, (pipeline_id, period, table_name, source_file, sheet_name, rows))
    
    conn.commit()


def get_loaded_periods(pipeline_id: str, conn) -> List[str]:
    """Get list of already-loaded periods."""
    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT DISTINCT period FROM {PIPELINES_SCHEMA}.tbl_load_history
        WHERE pipeline_id = %s
        ORDER BY period DESC
    """, (pipeline_id,))
    
    return [row[0] for row in cur.fetchall()]


# =============================================================================
# File Discovery & Classification
# =============================================================================

def discover_and_group(url: str) -> Tuple[Dict[str, List[Dict]], str]:
    """Discover files and group by period. Returns (periods_dict, latest_period)."""
    raw_files = scrape_landing_page(url, publication_id='temp', follow_links=True)
    
    periods = defaultdict(list)
    
    for f in raw_files:
        file_info = {
            'url': f.file_url,
            'filename': f.filename,
            'file_type': f.file_type,
            'period': f.period,
            'title': f.title,
        }
        
        period = f.period or 'unknown'
        periods[period].append(file_info)
    
    # Find latest
    valid_periods = [p for p in periods.keys() if p != 'unknown']
    latest = sorted(valid_periods, reverse=True)[0] if valid_periods else None
    
    return dict(periods), latest


def classify_file(filename: str) -> str:
    """Classify file as data, supporting, or methodology."""
    name_lower = filename.lower()
    
    if any(x in name_lower for x in ['methodology', 'technical', 'guidance', 'notes']):
        return 'methodology'
    if any(x in name_lower for x in ['summary', 'headline', 'dashboard', 'presentation']):
        return 'supporting'
    
    return 'data'


# =============================================================================
# Loading Functions
# =============================================================================

def sanitize_name(name: str, max_len: int = 63) -> str:
    """Create PostgreSQL-safe identifier."""
    clean = re.sub(r'[^a-z0-9]+', '_', str(name).lower())
    clean = re.sub(r'^_|_$', '', clean)
    clean = re.sub(r'_+', '_', clean)
    if clean and clean[0].isdigit():
        clean = 'c_' + clean
    return clean[:max_len] if clean else 'unnamed'


def download_file(url: str, filename: str) -> Optional[Path]:
    """Download file to local cache."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    local_path = DOWNLOAD_DIR / filename
    
    if local_path.exists():
        return local_path
    
    try:
        result = download_file_to_path(url, str(local_path))
        return Path(result) if result else None
    except Exception as e:
        console.print(f"[red]Download failed: {e}[/red]")
        return None


def get_sheet_info(file_path: Path) -> List[Dict]:
    """Get info about valid sheets in file."""
    import openpyxl
    
    sheets = []
    wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
    
    for sheet_name in wb.sheetnames:
        try:
            extractor = FileExtractor(str(file_path), sheet_name=sheet_name)
            structure = extractor.analyze_structure()
            
            if structure.is_valid:
                # Get column info
                columns = {
                    sanitize_name(c.pg_name): {
                        'original': c.original_headers,
                        'pg_name': c.pg_name,
                        'inferred_type': c.inferred_type
                    }
                    for c in structure.columns.values()
                }
                
                sheets.append({
                    'name': sheet_name,
                    'rows': structure.total_data_rows,
                    'cols': structure.total_data_cols,
                    'columns': columns
                })
        except:
            pass
    
    wb.close()
    return sheets


def load_sheet(file_path: Path, sheet_name: str, table_name: str, 
               period: str, column_mappings: Dict[str, str], conn) -> Tuple[bool, int, Dict]:
    """
    Load sheet to table using column mappings.
    Returns (success, rows_loaded, learned_columns).
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        if df.empty:
            return False, 0, {}
        
        # Sanitize and map columns
        final_columns = {}
        col_types = {}
        
        for orig_col in df.columns:
            sanitized = sanitize_name(str(orig_col))
            if not sanitized or sanitized.startswith('unnamed'):
                continue
            
            # Use mapping if exists, otherwise use sanitized name
            canonical = column_mappings.get(sanitized, sanitized)
            final_columns[orig_col] = canonical
            
            # Infer type
            dtype = df[orig_col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                col_types[canonical] = 'BIGINT'
            elif pd.api.types.is_float_dtype(dtype):
                col_types[canonical] = 'DOUBLE PRECISION'
            else:
                col_types[canonical] = 'TEXT'
        
        # Filter and rename columns
        df = df[[c for c in df.columns if c in final_columns]]
        df.columns = [final_columns[c] for c in df.columns]
        
        # Add metadata
        df['_period'] = period
        df['_source_file'] = file_path.name
        df['_sheet_name'] = sheet_name
        df['_loaded_at'] = datetime.now().isoformat()
        
        # Ensure table exists with correct schema
        full_table = f"{STAGING_SCHEMA}.{table_name}"
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{STAGING_SCHEMA}' AND table_name = '{table_name}'
        """)
        existing_cols = {row[0] for row in cur.fetchall()}
        
        if not existing_cols:
            # Create new table
            col_defs = ['_row_id SERIAL PRIMARY KEY']
            for col in df.columns:
                pg_type = col_types.get(col, 'TEXT')
                col_defs.append(f'"{col}" {pg_type}')
            
            cur.execute(f"CREATE TABLE {full_table} ({', '.join(col_defs)})")
            conn.commit()
        else:
            # Add any missing columns
            for col in df.columns:
                if col not in existing_cols and col != '_row_id':
                    pg_type = col_types.get(col, 'TEXT')
                    cur.execute(f'ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS "{col}" {pg_type}')
            conn.commit()
        
        # Load data using COPY
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep='\\N')
        buffer.seek(0)
        
        copy_cols = ', '.join(f'"{c}"' for c in df.columns)
        cur.copy_expert(f"COPY {full_table} ({copy_cols}) FROM STDIN WITH CSV NULL '\\N'", buffer)
        conn.commit()
        
        # Return learned column mappings
        learned = {sanitize_name(k): v for k, v in final_columns.items()}
        
        return True, len(df), {'mappings': learned, 'types': col_types}
        
    except Exception as e:
        logger.error(f"Load failed: {e}")
        return False, 0, {}


# =============================================================================
# Bootstrap Flow
# =============================================================================

def run_bootstrap(url: str, pipeline_id: str = None):
    """Interactive bootstrap: discover → select → load → save config."""
    
    console.print(Panel("[bold cyan]Pipeline Bootstrap[/bold cyan]\nThis will discover files, let you select what to load, and save the pattern for future auto-loads."))
    
    # Step 1: Discover and group
    console.print(f"\n[bold]Step 1: Discovering files...[/bold]")
    periods, latest = discover_and_group(url)
    
    console.print(f"[green]Found {sum(len(f) for f in periods.values())} files across {len(periods)} periods[/green]")
    console.print(f"[green]Latest period: {latest}[/green]")
    
    # Step 2: Select period for bootstrapping
    console.print(f"\n[bold]Step 2: Select bootstrap period[/bold]")
    console.print("[dim]We'll use this period to learn the file/sheet pattern, then apply it to all periods.[/dim]")
    
    period = Prompt.ask("Period to use for learning", default=latest)
    
    if period not in periods:
        console.print(f"[red]Period {period} not found[/red]")
        return
    
    files = periods[period]
    data_files = [f for f in files if classify_file(f['filename']) == 'data']
    
    console.print(f"\n[bold]Data files in {period}:[/bold]")
    for i, f in enumerate(data_files, 1):
        console.print(f"  {i}. {f['filename']} ({f['file_type']})")
    
    # Step 3: Select files to include
    console.print(f"\n[bold]Step 3: Select files to include in pipeline[/bold]")
    selection = Prompt.ask("File numbers (e.g., '1,2') or 'all'", default="all")
    
    if selection.lower() == 'all':
        selected_files = data_files
    else:
        indices = [int(x.strip()) - 1 for x in selection.split(',')]
        selected_files = [data_files[i] for i in indices if 0 <= i < len(data_files)]
    
    # Step 4: Analyze selected files and select sheets
    console.print(f"\n[bold]Step 4: Analyzing files and selecting sheets...[/bold]")
    
    file_patterns = []
    conn = get_connection()
    ensure_pipeline_schema(conn)
    
    try:
        for file_info in selected_files:
            console.print(f"\n[cyan]File: {file_info['filename']}[/cyan]")
            
            # Download
            local_path = download_file(file_info['url'], file_info['filename'])
            if not local_path:
                continue
            
            # Get sheets
            sheets = get_sheet_info(local_path)
            if not sheets:
                console.print("[yellow]  No valid sheets[/yellow]")
                continue
            
            console.print(f"  Sheets found:")
            for i, s in enumerate(sheets, 1):
                console.print(f"    {i}. {s['name']} ({s['rows']} rows, {s['cols']} cols)")
            
            # Select sheets
            sheet_selection = Prompt.ask("  Sheets to include", default="all")
            
            if sheet_selection.lower() == 'all':
                selected_sheets = sheets
            else:
                indices = [int(x.strip()) - 1 for x in sheet_selection.split(',')]
                selected_sheets = [sheets[i] for i in indices if 0 <= i < len(sheets)]
            
            # Create file pattern
            sheet_mappings = []
            
            for sheet in selected_sheets:
                # Generate table name
                file_base = sanitize_name(local_path.stem)
                sheet_base = sanitize_name(sheet['name'])
                default_table = f"tbl_{file_base}_{sheet_base}"[:63]
                
                table_name = Prompt.ask(f"    Table for '{sheet['name']}'", default=default_table)
                
                # Load the data for this bootstrap period
                console.print(f"    [dim]Loading to {table_name}...[/dim]")
                success, rows, learned = load_sheet(
                    local_path, sheet['name'], table_name, period, {}, conn
                )
                
                if success:
                    console.print(f"    [green]✓ Loaded {rows} rows[/green]")
                    
                    # Record the load
                    record_load(pipeline_id or 'bootstrap', period, table_name, 
                               file_info['filename'], sheet['name'], rows, conn)
                    
                    # Save sheet mapping
                    sheet_mappings.append(SheetMapping(
                        sheet_pattern=sheet['name'],  # Exact match for now
                        table_name=table_name,
                        column_mappings=learned.get('mappings', {}),
                        column_types=learned.get('types', {}),
                        is_primary=True
                    ))
                else:
                    console.print(f"    [red]✗ Load failed[/red]")
            
            if sheet_mappings:
                # Create filename pattern from this file
                # Use the base name without period-specific parts
                pattern = re.sub(r'\d{4}[-_]?\d{2}', r'\\d{4}[-_]?\\d{2}', file_info['filename'])
                pattern = re.sub(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[-_]?\d{2,4}', 
                                r'\\w+[-_]?\\d{2,4}', pattern, flags=re.IGNORECASE)
                
                file_patterns.append(FilePattern(
                    filename_pattern=pattern,
                    file_types=[file_info['file_type']] if file_info['file_type'] else ['xlsx'],
                    sheet_mappings=sheet_mappings,
                    category='data'
                ))
        
        # Step 5: Save pipeline configuration
        if not file_patterns:
            console.print("[red]No patterns learned. Aborting.[/red]")
            return
        
        if not pipeline_id:
            pipeline_id = Prompt.ask("\n[bold]Step 5: Pipeline ID[/bold]", 
                                     default=sanitize_name(Path(url).name))
        
        pipeline_name = Prompt.ask("Pipeline name", 
                                   default=pipeline_id.replace('_', ' ').title())
        
        config = PipelineConfig(
            pipeline_id=pipeline_id,
            name=pipeline_name,
            landing_page=url,
            file_patterns=file_patterns,
            loaded_periods=[period],
            auto_load=Confirm.ask("Enable auto-load for future periods?", default=True)
        )
        
        save_pipeline(config, conn)
        
        # Summary
        console.print(Panel(
            f"[bold green]Bootstrap Complete![/bold green]\n\n"
            f"Pipeline: {pipeline_id}\n"
            f"File patterns: {len(file_patterns)}\n"
            f"Tables created: {sum(len(fp.sheet_mappings) for fp in file_patterns)}\n"
            f"Bootstrap period: {period}\n\n"
            f"[dim]To load more periods:[/dim]\n"
            f"  python scripts/pipeline.py scan --pipeline {pipeline_id}\n\n"
            f"[dim]To load all history:[/dim]\n"
            f"  python scripts/pipeline.py backfill --pipeline {pipeline_id}",
            title="Summary"
        ))
        
    finally:
        conn.close()


# =============================================================================
# Scan & Auto-load Flow
# =============================================================================

def run_scan(pipeline_id: str, dry_run: bool = False):
    """Scan for new periods and auto-load if enabled."""
    
    conn = get_connection()
    ensure_pipeline_schema(conn)
    
    try:
        # Load pipeline config
        config = load_pipeline(pipeline_id, conn)
        if not config:
            console.print(f"[red]Pipeline '{pipeline_id}' not found[/red]")
            return
        
        console.print(f"[bold]Scanning: {config.name}[/bold]")
        console.print(f"[dim]URL: {config.landing_page}[/dim]")
        
        # Discover current files
        periods, latest = discover_and_group(config.landing_page)
        
        # Get already loaded periods
        loaded = set(get_loaded_periods(pipeline_id, conn))
        
        # Find new periods
        new_periods = sorted([p for p in periods.keys() if p != 'unknown' and p not in loaded])
        
        if not new_periods:
            console.print("[green]✓ No new periods to load[/green]")
            return
        
        console.print(f"[yellow]Found {len(new_periods)} new period(s): {', '.join(new_periods)}[/yellow]")
        
        if dry_run:
            console.print("[dim]Dry run - not loading[/dim]")
            return
        
        if not config.auto_load:
            if not Confirm.ask("Load new periods?"):
                return
        
        # Load each new period
        for period in new_periods:
            console.print(f"\n[cyan]Loading period: {period}[/cyan]")
            
            files = periods[period]
            
            for file_pattern in config.file_patterns:
                # Find matching files
                pattern_re = re.compile(file_pattern.filename_pattern, re.IGNORECASE)
                matching_files = [f for f in files if pattern_re.search(f['filename'])]
                
                if not matching_files:
                    continue
                
                for file_info in matching_files:
                    local_path = download_file(file_info['url'], file_info['filename'])
                    if not local_path:
                        continue
                    
                    for sheet_mapping in file_pattern.sheet_mappings:
                        # Load sheet using saved mapping
                        success, rows, _ = load_sheet(
                            local_path,
                            sheet_mapping.sheet_pattern,
                            sheet_mapping.table_name,
                            period,
                            sheet_mapping.column_mappings,
                            conn
                        )
                        
                        if success:
                            console.print(f"  [green]✓ {sheet_mapping.table_name}: {rows} rows[/green]")
                            record_load(pipeline_id, period, sheet_mapping.table_name,
                                       file_info['filename'], sheet_mapping.sheet_pattern, rows, conn)
                        else:
                            console.print(f"  [red]✗ {sheet_mapping.sheet_pattern} failed[/red]")
        
        # Update last scan time
        config.last_scan_at = datetime.now().isoformat()
        save_pipeline(config, conn)
        
        console.print(f"\n[green]✓ Scan complete[/green]")
        
    finally:
        conn.close()


def run_backfill(pipeline_id: str, from_period: str = None):
    """Load all historical periods for a pipeline."""
    
    conn = get_connection()
    ensure_pipeline_schema(conn)
    
    try:
        config = load_pipeline(pipeline_id, conn)
        if not config:
            console.print(f"[red]Pipeline '{pipeline_id}' not found[/red]")
            return
        
        console.print(f"[bold]Backfilling: {config.name}[/bold]")
        
        # Discover all periods
        periods, _ = discover_and_group(config.landing_page)
        loaded = set(get_loaded_periods(pipeline_id, conn))
        
        # Filter periods
        all_periods = sorted([p for p in periods.keys() if p != 'unknown'])
        if from_period:
            all_periods = [p for p in all_periods if p >= from_period]
        
        to_load = [p for p in all_periods if p not in loaded]
        
        console.print(f"Periods available: {len(all_periods)}")
        console.print(f"Already loaded: {len(loaded)}")
        console.print(f"To backfill: {len(to_load)}")
        
        if not to_load:
            console.print("[green]Nothing to backfill[/green]")
            return
        
        if not Confirm.ask(f"Load {len(to_load)} periods?"):
            return
        
        # Load each period
        for period in to_load:
            console.print(f"\n[cyan]Period: {period}[/cyan]")
            
            files = periods[period]
            
            for file_pattern in config.file_patterns:
                pattern_re = re.compile(file_pattern.filename_pattern, re.IGNORECASE)
                matching_files = [f for f in files if pattern_re.search(f['filename'])]
                
                for file_info in matching_files:
                    local_path = download_file(file_info['url'], file_info['filename'])
                    if not local_path:
                        continue
                    
                    for sheet_mapping in file_pattern.sheet_mappings:
                        success, rows, _ = load_sheet(
                            local_path,
                            sheet_mapping.sheet_pattern,
                            sheet_mapping.table_name,
                            period,
                            sheet_mapping.column_mappings,
                            conn
                        )
                        
                        if success:
                            console.print(f"  [green]✓ {sheet_mapping.table_name}: {rows}[/green]")
                            record_load(pipeline_id, period, sheet_mapping.table_name,
                                       file_info['filename'], sheet_mapping.sheet_pattern, rows, conn)
        
        console.print(f"\n[green]✓ Backfill complete[/green]")
        
    finally:
        conn.close()


def run_list():
    """List all registered pipelines."""
    conn = get_connection()
    ensure_pipeline_schema(conn)
    
    try:
        pipelines = list_pipelines(conn)
        
        if not pipelines:
            console.print("[yellow]No pipelines registered yet.[/yellow]")
            console.print("\nTo create one:")
            console.print("  python scripts/pipeline.py bootstrap --url <url>")
            return
        
        table = Table(title="Registered Pipelines")
        table.add_column("ID", style="cyan")
        table.add_column("Name")
        table.add_column("Patterns")
        table.add_column("Periods Loaded")
        table.add_column("Auto-load")
        
        for p in pipelines:
            loaded_periods = get_loaded_periods(p.pipeline_id, conn)
            table.add_row(
                p.pipeline_id,
                p.name,
                str(len(p.file_patterns)),
                str(len(loaded_periods)),
                "✓" if p.auto_load else ""
            )
        
        console.print(table)
        
    finally:
        conn.close()


# =============================================================================
# Main CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="DataWarp Pipeline Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  bootstrap   Create a new pipeline interactively
  scan        Check for new periods and load if found
  backfill    Load all historical periods
  list        Show all registered pipelines

Examples:
  # Create new pipeline
  python scripts/pipeline.py bootstrap --url "https://digital.nhs.uk/.../mi-adhd"
  
  # Check for new data
  python scripts/pipeline.py scan --pipeline adhd
  
  # Load all history
  python scripts/pipeline.py backfill --pipeline adhd --from 2023-01
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    # Bootstrap
    boot = subparsers.add_parser('bootstrap', help='Create new pipeline')
    boot.add_argument('--url', required=True, help='Publication landing page URL')
    boot.add_argument('--id', dest='pipeline_id', help='Pipeline ID (auto-generated if omitted)')
    
    # Scan
    scan = subparsers.add_parser('scan', help='Scan for new periods')
    scan.add_argument('--pipeline', required=True, help='Pipeline ID')
    scan.add_argument('--dry-run', action='store_true', help='Show what would be loaded')
    
    # Backfill
    back = subparsers.add_parser('backfill', help='Load historical periods')
    back.add_argument('--pipeline', required=True, help='Pipeline ID')
    back.add_argument('--from', dest='from_period', help='Start from period (YYYY-MM)')
    
    # List
    subparsers.add_parser('list', help='List pipelines')
    
    args = parser.parse_args()
    
    if args.command == 'bootstrap':
        run_bootstrap(args.url, args.pipeline_id)
    elif args.command == 'scan':
        run_scan(args.pipeline, args.dry_run)
    elif args.command == 'backfill':
        run_backfill(args.pipeline, args.from_period)
    elif args.command == 'list':
        run_list()


if __name__ == '__main__':
    main()
