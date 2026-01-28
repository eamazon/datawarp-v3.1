"""
Add-sheet command - add a new sheet to an existing pipeline.
"""
import os
import re
import tempfile
from typing import Optional

import click

from datawarp.cli.console import console
from datawarp.cli.file_processor import process_data_file
from datawarp.discovery import scrape_landing_page
from datawarp.loader import download_file, get_sheet_names, FileExtractor
from datawarp.metadata import detect_grain, enrich_sheet
from datawarp.pipeline import load_config, save_config, SheetMapping, record_load
from datawarp.utils import sanitize_name, make_table_name


@click.command('add-sheet')
@click.option('--pipeline', required=True, help='Pipeline ID to add sheet to')
@click.option('--sheet', required=True, help='Sheet name to add')
@click.option('--file-pattern', default=None, help='File pattern index (default: first xlsx pattern)')
@click.option('--enrich/--no-enrich', default=True, help='Use LLM enrichment')
@click.option('--period', default=None, help='Period to load (default: latest)')
def add_sheet_command(pipeline: str, sheet: str, file_pattern: Optional[int], enrich: bool, period: Optional[str]):
    """
    Add a new sheet from an existing file to a pipeline.

    Useful when NHS adds a new sheet to an existing Excel file that wasn't
    included in the original bootstrap.

    Examples:
        # Add a sheet from the latest file
        python scripts/pipeline.py add-sheet --pipeline mi_adhd --sheet "New Data"

        # Add without LLM enrichment
        python scripts/pipeline.py add-sheet --pipeline mi_adhd --sheet "Raw Data" --no-enrich
    """
    config = load_config(pipeline)
    if not config:
        console.print(f"[error]Pipeline '{pipeline}' not found[/]")
        return

    console.print(f"\n[info]Adding sheet to:[/] {config.name}")
    console.print(f"[muted]Sheet: {sheet}[/]\n")

    # Check if sheet already exists in config
    for fp in config.file_patterns:
        for sm in fp.sheet_mappings:
            if sm.sheet_pattern == sheet:
                console.print(f"[warning]Sheet '{sheet}' already exists in pipeline[/]")
                console.print(f"  Table: {sm.table_name}")
                return

    # Find the file pattern to add to (default: first xlsx pattern)
    target_fp = None
    if file_pattern is not None:
        if file_pattern < len(config.file_patterns):
            target_fp = config.file_patterns[file_pattern]
        else:
            console.print(f"[error]File pattern index {file_pattern} out of range[/]")
            return
    else:
        # Find first xlsx pattern
        for fp in config.file_patterns:
            if 'xlsx' in fp.file_types or 'xls' in fp.file_types:
                target_fp = fp
                break

    if not target_fp:
        console.print("[error]No Excel file pattern found in pipeline[/]")
        return

    console.print(f"[muted]File patterns: {target_fp.filename_patterns}[/]")

    # Discover latest file
    console.print("\n[muted]Discovering files...[/]")
    files = scrape_landing_page(config.landing_page)

    # Filter to matching files (match ANY pattern)
    matching = [f for f in files
                if any(re.match(p, f.filename, re.IGNORECASE) for p in target_fp.filename_patterns)]

    if not matching:
        console.print(f"[error]No files matching patterns: {target_fp.filename_patterns}[/]")
        return

    # Get latest (or specified period)
    if period:
        target_file = next((f for f in matching if f.period == period), None)
        if not target_file:
            console.print(f"[error]No file found for period {period}[/]")
            return
    else:
        # Sort by period descending, take first
        matching.sort(key=lambda f: f.period or '', reverse=True)
        target_file = matching[0]
        period = target_file.period

    console.print(f"[muted]Using file: {target_file.filename} (period: {period})[/]")

    # Download file
    temp_dir = tempfile.mkdtemp()
    with console.status("Downloading..."):
        local_path = download_file(target_file.url, temp_dir)

    # Check sheet exists
    sheets = get_sheet_names(local_path)
    if sheet not in sheets:
        console.print(f"[error]Sheet '{sheet}' not found in file[/]")
        console.print(f"[muted]Available sheets: {', '.join(sheets)}[/]")
        return

    console.print(f"[success]✓ Sheet found[/]")

    # Extract data
    try:
        extractor = FileExtractor(local_path, sheet)
        df = extractor.to_dataframe()
    except Exception as e:
        console.print(f"[error]Error reading sheet: {e}[/]")
        return

    if df.empty:
        console.print("[warning]Sheet is empty, skipping[/]")
        return

    console.print(f"[muted]Rows: {len(df)}, Columns: {len(df.columns)}[/]")

    # Detect grain
    grain_info = detect_grain(df)
    grain = grain_info['grain']
    grain_col = grain_info['grain_column']
    grain_desc = grain_info['description']
    console.print(f"[muted]Grain: {grain} ({grain_desc})[/]")

    # Generate table name and mappings
    table_name = make_table_name(pipeline, sheet)
    table_desc = f"Data from {sheet}"
    sanitized_cols = [sanitize_name(str(c)) for c in df.columns]
    col_mappings = {c: c for c in sanitized_cols}
    col_descriptions = {}

    # Enrich if requested
    if enrich:
        console.print("[warning]Enriching with LLM...[/]")
        enriched = enrich_sheet(
            sheet_name=sheet,
            columns=sanitized_cols,
            sample_rows=df.head(3).to_dict('records'),
            publication_hint=config.name,
            grain_hint=grain,
            pipeline_id=pipeline,
            source_file=target_file.filename
        )
        table_name = f"tbl_{sanitize_name(enriched['table_name'])}"[:63]
        table_desc = enriched['table_description']
        col_mappings = enriched['columns']
        col_descriptions = enriched['descriptions']
        console.print(f"[success]LLM suggested: {table_name}[/]")

    # Load data
    from datawarp.loader import load_sheet as load_sheet_fn

    console.print(f"\n[muted]Loading to {table_name}...[/]")
    rows, learned_mappings, col_types = load_sheet_fn(
        local_path, sheet, table_name,
        period=period, column_mappings=col_mappings
    )

    if rows == 0:
        console.print("[warning]No rows loaded[/]")
        return

    console.print(f"[success]✓ Loaded {rows} rows[/]")

    # Create SheetMapping
    new_mapping = SheetMapping(
        sheet_pattern=sheet,
        table_name=table_name,
        table_description=table_desc,
        column_mappings=learned_mappings,
        column_descriptions=col_descriptions,
        column_types=col_types,
        grain=grain,
        grain_column=grain_col,
        grain_description=grain_desc,
    )

    # Add to file pattern
    target_fp.sheet_mappings.append(new_mapping)

    # Record load
    record_load(pipeline, period, table_name, target_file.filename, sheet, rows)

    # Save config
    save_config(config)

    console.print(f"\n[success]✓ Sheet '{sheet}' added to pipeline[/]")
    console.print(f"[muted]Table: {table_name}[/]")
    console.print(f"[muted]Config saved[/]")
