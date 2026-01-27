#!/usr/bin/env python3
"""
DataWarp v3.1 CLI - Pipeline management

Commands:
    bootstrap   Learn pattern from NHS URL and load latest period
    scan        Find and load new periods for a pipeline
    backfill    Load all historical periods
    list        List registered pipelines
    history     Show load history for a pipeline
"""
import os
import re
import sys
import tempfile
from typing import List, Optional, Tuple

import click
import requests
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.panel import Panel

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datawarp.discovery import scrape_landing_page, DiscoveredFile, classify_url, URLClassification, generate_period_urls
from datawarp.loader import load_sheet, load_file, download_file, get_sheet_names, preview_sheet, FileExtractor, extract_zip, list_zip_contents
from datawarp.pipeline import (
    PipelineConfig, FilePattern, SheetMapping,
    save_config, load_config, list_configs, record_load, get_load_history
)
from datawarp.utils import parse_period, extract_periods_from_files, sanitize_name, make_table_name
from datawarp.storage import get_connection
from datawarp.metadata import detect_grain, enrich_sheet

console = Console()


def _group_files_by_period(files: List) -> dict:
    """Group DiscoveredFile objects by their period attribute."""
    by_period = {}
    for f in files:
        period = f.period or 'unknown'
        if period not in by_period:
            by_period[period] = []
        by_period[period].append({'filename': f.filename, 'url': f.url, 'file': f})
    return by_period


def _process_data_file(
    local_path: str,
    filename: str,
    file_type: str,
    period: str,
    auto_id: str,
    enrich: bool,
    console,
) -> List[Tuple[SheetMapping, int]]:
    """
    Process a single data file (CSV, Excel, or ZIP) and return sheet mappings.

    For ZIP files, extracts and recursively processes each data file inside.

    Returns:
        List of (SheetMapping, rows_loaded) tuples
    """
    import pandas as pd
    from datawarp.metadata import detect_grain, enrich_sheet

    results = []

    if file_type == 'zip':
        # Extract ZIP and process each file inside
        console.print(f"  [dim]Extracting ZIP contents...[/]")
        zip_contents = list_zip_contents(local_path)
        console.print(f"  Found {len(zip_contents)} data files in ZIP")

        extracted_files = extract_zip(local_path)
        for extracted_path in extracted_files:
            ext_filename = os.path.basename(extracted_path)
            ext_type = os.path.splitext(ext_filename)[1].lower().lstrip('.')
            console.print(f"\n  [cyan]→ {ext_filename}[/]")

            # Recursively process extracted file
            sub_results = _process_data_file(
                extracted_path, ext_filename, ext_type,
                period, auto_id, enrich, console
            )
            results.extend(sub_results)

        return results

    elif file_type in ['xlsx', 'xls']:
        # Excel file - use FileExtractor for each sheet
        sheets = get_sheet_names(local_path)
        console.print(f"  {len(sheets)} sheet(s)")

        for sheet in sheets:
            try:
                extractor = FileExtractor(local_path, sheet)
                df = extractor.to_dataframe()

                if df.empty:
                    continue

                grain_info = detect_grain(df)
                grain = grain_info['grain']
                grain_col = grain_info['grain_column']
                grain_desc = grain_info['description']

                table_suffix = sanitize_name(sheet)
                table_name = make_table_name(auto_id, sheet)
                table_desc = f"Data from {sheet}"

                sanitized_cols = [sanitize_name(str(c)) for c in df.columns]
                col_mappings = {c: c for c in sanitized_cols}
                col_descriptions = {}

                # Enrichment (optional)
                if enrich:
                    sample_rows = df.head(3).to_dict('records')
                    enriched = enrich_sheet(
                        sheet_name=sheet,
                        columns=sanitized_cols,
                        sample_rows=sample_rows,
                        publication_hint=auto_id,
                        grain_hint=grain,
                        pipeline_id=auto_id,
                        source_file=local_path
                    )
                    # Use LLM name directly - it should be short and semantic
                    table_suffix = enriched['table_name']
                    table_name = f"tbl_{sanitize_name(table_suffix)}"[:63]
                    table_desc = enriched['table_description']
                    col_mappings = enriched['columns']
                    col_descriptions = enriched['descriptions']
                    console.print(f"    [green]LLM suggested: {table_name}[/]")

                rows, learned_mappings, col_types = load_sheet(
                    local_path, sheet, table_name,
                    period=period, column_mappings=col_mappings
                )

                if rows > 0:
                    console.print(f"    {sheet}: {rows} rows ({grain})")
                    results.append((SheetMapping(
                        sheet_pattern=sheet,
                        table_name=table_name,
                        table_description=table_desc,
                        column_mappings=learned_mappings,
                        column_descriptions=col_descriptions,
                        column_types=col_types,
                        grain=grain,
                        grain_column=grain_col,
                        grain_description=grain_desc,
                    ), rows))

            except Exception as e:
                console.print(f"    [dim]{sheet}: skipped ({e})[/]")

        return results

    else:
        # CSV file
        try:
            df = pd.read_csv(local_path, low_memory=False)
        except Exception as e:
            console.print(f"  [red]Error reading: {e}[/]")
            return results

        grain_info = detect_grain(df)
        grain = grain_info['grain']
        grain_col = grain_info['grain_column']
        grain_desc = grain_info['description']

        table_suffix = sanitize_name(os.path.splitext(filename)[0])
        table_name = make_table_name(auto_id, table_suffix)
        table_desc = f"Data from {filename}"

        sanitized_cols = [sanitize_name(str(c)) for c in df.columns]
        col_mappings = {c: c for c in sanitized_cols}
        col_descriptions = {}

        if enrich:
            console.print(f"  [yellow]Enriching with LLM...[/]")
            sample_rows = df.head(3).to_dict('records')
            enriched = enrich_sheet(
                sheet_name=os.path.splitext(filename)[0],
                columns=sanitized_cols,
                sample_rows=sample_rows,
                publication_hint=auto_id,
                grain_hint=grain,
                pipeline_id=auto_id,
                source_file=local_path
            )
            # Use LLM name directly - it should be short and semantic
            table_suffix = enriched['table_name']
            table_name = f"tbl_{sanitize_name(table_suffix)}"[:63]
            table_desc = enriched['table_description']
            col_mappings = enriched['columns']
            col_descriptions = enriched['descriptions']
            console.print(f"  [green]LLM suggested: {table_name}[/]")

        rows, learned_mappings, col_types = load_file(
            local_path, table_name,
            period=period, column_mappings=col_mappings
        )

        if rows > 0:
            console.print(f"  Loaded {rows} rows ({grain})")
            results.append((SheetMapping(
                sheet_pattern='',
                table_name=table_name,
                table_description=table_desc,
                column_mappings=learned_mappings,
                column_descriptions=col_descriptions,
                column_types=col_types,
                grain=grain,
                grain_column=grain_col,
                grain_description=grain_desc,
            ), rows))

        return results


@click.group()
def cli():
    """DataWarp v3.1 - NHS Data Pipeline"""
    pass


@cli.command()
@click.option('--url', required=True, help='NHS publication landing page URL')
@click.option('--name', help='Pipeline name (auto-generated if not provided)')
@click.option('--id', 'pipeline_id', help='Pipeline ID (auto-generated if not provided)')
@click.option('--enrich', is_flag=True, help='Use LLM to generate semantic column names')
@click.option('--skip-unknown/--no-skip-unknown', default=True, help='Skip sheets with no detected entity')
def bootstrap(url: str, name: Optional[str], pipeline_id: Optional[str], enrich: bool, skip_unknown: bool):
    """
    Bootstrap a new pipeline from an NHS URL.

    Discovers files, groups by period, lets you select what to load,
    then saves the pattern for future scans.

    Use --enrich to call LLM for semantic column names and descriptions.
    """
    # Step 1: Classify URL to determine discovery strategy
    console.print(f"\n[bold blue]Classifying URL...[/]")
    with console.status("Analyzing URL structure..."):
        classification = classify_url(url)

    # Show classification info
    console.print(Panel(
        f"[bold]{classification.name}[/]\n"
        f"ID: {classification.publication_id}\n"
        f"Source: {classification.source}\n"
        f"Discovery: [cyan]{classification.discovery_mode}[/]\n"
        f"Frequency: {classification.frequency}" +
        (f"\nURL Pattern: {classification.url_pattern}" if classification.url_pattern else "") +
        (f"\n[yellow]⚠ NHS Digital page with NHS England data[/]" if classification.redirects_to_england else ""),
        title="URL Classification"
    ))

    # Handle explicit mode (hash-coded URLs)
    if classification.discovery_mode == 'explicit':
        console.print("[yellow]This publication uses hash-coded URLs that cannot be auto-discovered.[/]")
        console.print("Please provide the exact file URL using --url with a direct file link.")
        return

    # Step 2: Discover files based on mode
    # If user gave a period-specific URL (e.g., /january-2026), use that directly
    if classification.is_period_url:
        scrape_url = classification.original_url
        console.print(f"\n[bold blue]Discovering files from period URL:[/] {scrape_url}\n")
    else:
        scrape_url = classification.landing_page
        console.print(f"\n[bold blue]Discovering files from:[/] {scrape_url}\n")

    with console.status("Scraping page..."):
        files = scrape_landing_page(scrape_url)

    if not files:
        console.print("[red]No data files found at this URL[/]")
        return

    console.print(f"[green]Found {len(files)} files[/]\n")

    # Step 2: Group by period (use the period already computed by scraper)
    by_period = _group_files_by_period(files)
    periods = sorted([p for p in by_period.keys() if p != 'unknown'], reverse=True)

    if not periods:
        console.print("[red]Could not detect periods from filenames[/]")
        return

    # Show period summary
    table = Table(title="Available Periods")
    table.add_column("Period", style="cyan")
    table.add_column("Files", justify="right")

    for period in periods[:10]:  # Show latest 10
        table.add_row(period, str(len(by_period[period])))

    if len(periods) > 10:
        table.add_row("...", f"({len(periods) - 10} more)")

    console.print(table)

    # Step 3: Select latest period
    latest = periods[0]
    console.print(f"\n[bold]Latest period: {latest}[/] ({len(by_period[latest])} files)")

    if not Confirm.ask("Bootstrap from this period?", default=True):
        latest = Prompt.ask("Enter period to bootstrap from", choices=periods)

    # Step 4: Show files in selected period
    period_files = [item['file'] for item in by_period[latest]]

    console.print(f"\n[bold]Files in {latest}:[/]")
    for i, f in enumerate(period_files, 1):
        console.print(f"  {i}. {f.filename} ({f.file_type})")

    # Step 5: Select files to include
    if len(period_files) == 1:
        selected_files = period_files
    else:
        selection = Prompt.ask(
            "\nSelect files to include (comma-separated numbers, or 'all')",
            default="all"
        )
        if selection.lower() == 'all':
            selected_files = period_files
        else:
            indices = [int(x.strip()) - 1 for x in selection.split(',')]
            selected_files = [period_files[i] for i in indices if 0 <= i < len(period_files)]

    if not selected_files:
        console.print("[red]No files selected[/]")
        return

    # Step 6: Process each file - select sheets and load
    file_patterns = []
    temp_dir = tempfile.mkdtemp()

    for f in selected_files:
        console.print(f"\n[bold cyan]Processing: {f.filename}[/]")

        # Download file
        with console.status("Downloading..."):
            local_path = download_file(f.url, temp_dir)

        if f.file_type in ['xlsx', 'xls']:
            # Get all sheets and preview each one
            sheets = get_sheet_names(local_path)

            # Build preview table with grain detection
            console.print(f"\n  [bold]Analyzing {len(sheets)} sheets...[/]")
            sheet_previews = []

            with console.status("Detecting sheet types..."):
                for sheet in sheets:
                    try:
                        extractor = FileExtractor(local_path, sheet)
                        structure = extractor.infer_structure()

                        if not structure.is_valid:
                            sheet_previews.append({
                                'name': sheet,
                                'grain': 'invalid',
                                'rows': 0,
                                'cols': 0,
                                'description': 'Could not parse structure',
                                'extractor': None,
                                'df': None,
                            })
                            continue

                        df = extractor.to_dataframe()

                        if df.empty:
                            sheet_previews.append({
                                'name': sheet,
                                'grain': 'empty',
                                'rows': 0,
                                'cols': 0,
                                'description': 'No data rows',
                                'extractor': None,
                                'df': None,
                            })
                            continue

                        # Detect grain
                        grain_info = detect_grain(df)

                        sheet_previews.append({
                            'name': sheet,
                            'grain': grain_info['grain'],
                            'rows': len(df),
                            'cols': len(df.columns),
                            'description': grain_info['description'] or _infer_sheet_description(sheet),
                            'extractor': extractor,
                            'df': df,
                            'grain_info': grain_info,
                        })
                    except Exception as e:
                        sheet_previews.append({
                            'name': sheet,
                            'grain': 'error',
                            'rows': 0,
                            'cols': 0,
                            'description': str(e)[:50],
                            'extractor': None,
                            'df': None,
                        })

            # Show preview table
            preview_table = Table(title=f"Sheets in {f.filename}")
            preview_table.add_column("#", style="dim", width=3)
            preview_table.add_column("Sheet Name", style="cyan")
            preview_table.add_column("Grain", style="green")
            preview_table.add_column("Rows", justify="right")
            preview_table.add_column("Cols", justify="right")
            preview_table.add_column("Description")

            data_sheets = []  # Sheets with actual data
            for i, sp in enumerate(sheet_previews, 1):
                grain_style = "green" if sp['grain'] not in ('unknown', 'empty', 'invalid', 'error') else "dim"
                is_data = sp['grain'] not in ('empty', 'invalid', 'error') and sp['rows'] > 0

                if is_data:
                    data_sheets.append(i)

                preview_table.add_row(
                    str(i) if is_data else f"[dim]{i}[/]",
                    sp['name'],
                    f"[{grain_style}]{sp['grain']}[/]",
                    str(sp['rows']) if sp['rows'] > 0 else "-",
                    str(sp['cols']) if sp['cols'] > 0 else "-",
                    sp['description'][:40] + "..." if len(sp['description']) > 40 else sp['description'],
                )

            console.print(preview_table)

            # Count sheets by type
            known_grain = [sp for sp in sheet_previews if sp['grain'] not in ('empty', 'invalid', 'error', 'unknown') and sp['rows'] > 0]
            unknown_grain = [sp for sp in sheet_previews if sp['grain'] == 'unknown' and sp['rows'] > 0]
            skip_sheets = [sp for sp in sheet_previews if sp['grain'] in ('empty', 'invalid', 'error')]

            console.print(f"\n  [green]{len(known_grain)} with entity[/], [yellow]{len(unknown_grain)} national/unknown[/], [dim]{len(skip_sheets)} skipped[/]")

            # Let user select sheets
            if len(sheets) == 1:
                selected_indices = [1]
            else:
                # Build smart default based on what's available
                if known_grain:
                    # Have sheets with detected entities - default to those
                    default_indices = [i for i, sp in enumerate(sheet_previews, 1)
                                      if sp['grain'] not in ('empty', 'invalid', 'error', 'unknown') and sp['rows'] > 0]
                    hint = "Defaulting to sheets with detected entities (ICB/Trust/etc)"
                elif unknown_grain:
                    # All data sheets are unknown - probably national aggregates, select all
                    default_indices = [i for i, sp in enumerate(sheet_previews, 1)
                                      if sp['grain'] not in ('empty', 'invalid', 'error') and sp['rows'] > 0]
                    hint = "No entity codes detected - selecting all data sheets (likely national aggregates)"
                else:
                    default_indices = []
                    hint = "No data sheets found"

                if hint:
                    console.print(f"  [dim]{hint}[/]")

                default_str = ','.join(map(str, default_indices)) if default_indices else ''

                selection = Prompt.ask(
                    f"\n  Select sheets (numbers, 'all', or enter for default)",
                    default=default_str
                )

                if selection.lower() == 'all':
                    # All sheets with data (excluding empty/invalid/error)
                    selected_indices = [i for i, sp in enumerate(sheet_previews, 1)
                                       if sp['grain'] not in ('empty', 'invalid', 'error') and sp['rows'] > 0]
                elif selection.lower() == 'data' or selection.lower() == 'known':
                    # Only sheets with detected entities
                    selected_indices = [i for i, sp in enumerate(sheet_previews, 1)
                                       if sp['grain'] not in ('empty', 'invalid', 'error', 'unknown') and sp['rows'] > 0]
                elif not selection.strip():
                    # Empty = use defaults
                    selected_indices = default_indices
                else:
                    try:
                        selected_indices = [int(x.strip()) for x in selection.split(',') if x.strip()]
                    except ValueError:
                        # Try matching sheet names
                        selected_indices = [i for i, sp in enumerate(sheet_previews, 1)
                                          if sp['name'] in selection]

            # Filter to valid selections with data
            selected_sheets_data = [
                sheet_previews[i-1] for i in selected_indices
                if 1 <= i <= len(sheet_previews) and sheet_previews[i-1]['df'] is not None
            ]

            if not selected_sheets_data:
                console.print("  [yellow]No valid sheets selected[/]")
                continue

            # Load each selected sheet
            sheet_mappings = []
            auto_name = name or classification.name
            auto_id = pipeline_id or classification.publication_id

            for sp in selected_sheets_data:
                sheet = sp['name']
                preview = sp['df']
                grain_info = sp['grain_info']

                console.print(f"\n  [bold]Loading: {sheet}[/] ({sp['rows']} rows, {sp['grain']})")

                grain = grain_info['grain']
                grain_col = grain_info['grain_column']
                grain_desc = grain_info['description']

                col_names = list(preview.columns[:5])
                console.print(f"  Columns: {', '.join(col_names)}{'...' if len(preview.columns) > 5 else ''}")

                # Prepare for enrichment
                sanitized_cols = [sanitize_name(str(c)) for c in preview.columns if not str(c).lower().startswith('unnamed')]
                sample_rows = preview.head(3).to_dict('records')

                # Enrichment (optional)
                if enrich:
                    console.print(f"  [yellow]Enriching with LLM...[/]")
                    enriched = enrich_sheet(
                        sheet_name=sheet,
                        columns=sanitized_cols,
                        sample_rows=sample_rows,
                        publication_hint=auto_id,
                        grain_hint=grain,
                        pipeline_id=auto_id,
                        source_file=local_path
                    )
                    # Use LLM name directly - it should be short and semantic
                    table_suffix = enriched['table_name']
                    table_name = f"tbl_{sanitize_name(table_suffix)}"[:63]
                    table_desc = enriched['table_description']
                    col_mappings = enriched['columns']
                    col_descriptions = enriched['descriptions']
                    console.print(f"  [green]LLM suggested: {table_name}[/]")
                else:
                    table_suffix = sanitize_name(sheet)
                    table_name = make_table_name(auto_id, table_suffix)
                    table_desc = f"Data from {sheet}"
                    col_mappings = {c: c for c in sanitized_cols}
                    col_descriptions = {}

                console.print(f"  [dim]Table: staging.{table_name}[/]")

                # Load data
                with console.status("Loading to database..."):
                    rows, learned_mappings, col_types = load_sheet(
                        local_path, sheet, table_name,
                        period=latest,
                        column_mappings=col_mappings
                    )

                if rows == 0:
                    console.print(f"  [dim]Skipped (no data or sheet not found)[/]")
                    continue

                console.print(f"  [green]Loaded {rows} rows[/]")

                # Record load
                record_load(auto_id, latest, table_name, f.filename, sheet, rows)

                # Save sheet mapping with enriched data
                sheet_mappings.append(SheetMapping(
                    sheet_pattern=sheet,
                    table_name=table_name,
                    table_description=table_desc,
                    column_mappings=learned_mappings,
                    column_descriptions=col_descriptions,
                    column_types=col_types,
                    grain=grain,
                    grain_column=grain_col,
                    grain_description=grain_desc,
                ))

            # Create file pattern
            file_patterns.append(FilePattern(
                filename_pattern=_make_filename_pattern(f.filename),
                file_types=[f.file_type],
                sheet_mappings=sheet_mappings,
            ))

        elif f.file_type == 'zip':
            # ZIP file - extract and process each data file inside
            auto_name = name or classification.name
            auto_id = pipeline_id or classification.publication_id

            console.print(f"  [dim]Extracting ZIP...[/]")
            zip_contents = list_zip_contents(local_path)
            console.print(f"  Found {len(zip_contents)} data file(s) inside")

            for item in zip_contents:
                console.print(f"    - {item['filename']} ({item['file_type']})")

            # Extract all files
            extracted_files = extract_zip(local_path)
            sheet_mappings = []

            for extracted_path in extracted_files:
                ext_filename = os.path.basename(extracted_path)
                ext_type = os.path.splitext(ext_filename)[1].lower().lstrip('.')
                console.print(f"\n  [cyan]Processing: {ext_filename}[/]")

                # Use the helper function for each extracted file
                results = _process_data_file(
                    extracted_path, ext_filename, ext_type,
                    latest, auto_id, enrich, console
                )

                for mapping, rows in results:
                    record_load(auto_id, latest, mapping.table_name, f.filename, mapping.sheet_pattern or ext_filename, rows)
                    sheet_mappings.append(mapping)

            if sheet_mappings:
                file_patterns.append(FilePattern(
                    filename_pattern=_make_filename_pattern(f.filename),
                    file_types=[f.file_type],
                    sheet_mappings=sheet_mappings,
                ))

        else:
            # CSV file
            import pandas as pd
            auto_name = name or classification.name
            auto_id = pipeline_id or classification.publication_id

            # Read preview for grain detection
            try:
                preview = pd.read_csv(local_path, nrows=50)
            except Exception as e:
                console.print(f"  [red]Error reading CSV: {e}[/]")
                continue

            # Detect grain
            grain_info = detect_grain(preview)
            grain = grain_info['grain']
            grain_col = grain_info['grain_column']
            grain_desc = grain_info['description']

            console.print(f"  Grain: [cyan]{grain}[/] ({grain_desc})")

            # Prepare for enrichment
            sanitized_cols = [sanitize_name(str(c)) for c in preview.columns if not str(c).lower().startswith('unnamed')]
            sample_rows = preview.head(3).to_dict('records')

            # Enrichment (optional)
            if enrich:
                console.print(f"  [yellow]Enriching with LLM...[/]")
                enriched = enrich_sheet(
                    sheet_name=os.path.splitext(f.filename)[0],
                    columns=sanitized_cols,
                    sample_rows=sample_rows,
                    publication_hint=auto_id,
                    grain_hint=grain,
                    pipeline_id=auto_id,
                    source_file=local_path
                )
                # Use LLM name directly - it should be short and semantic
                table_suffix = enriched['table_name']
                table_name = f"tbl_{sanitize_name(table_suffix)}"[:63]
                table_desc = enriched['table_description']
                col_mappings = enriched['columns']
                col_descriptions = enriched['descriptions']
                console.print(f"  [green]LLM suggested: {table_name}[/]")
            else:
                table_suffix = sanitize_name(os.path.splitext(f.filename)[0])
                table_name = make_table_name(auto_id, table_suffix)
                table_desc = f"Data from {f.filename}"
                col_mappings = {c: c for c in sanitized_cols}
                col_descriptions = {}

            with console.status("Loading to database..."):
                rows, learned_mappings, col_types = load_file(
                    local_path, table_name,
                    period=latest,
                    column_mappings=col_mappings
                )

            console.print(f"  [green]Loaded {rows} rows to staging.{table_name}[/]")
            record_load(auto_id, latest, table_name, f.filename, None, rows)

            file_patterns.append(FilePattern(
                filename_pattern=_make_filename_pattern(f.filename),
                file_types=[f.file_type],
                sheet_mappings=[SheetMapping(
                    sheet_pattern='',
                    table_name=table_name,
                    table_description=table_desc,
                    column_mappings=learned_mappings,
                    column_descriptions=col_descriptions,
                    column_types=col_types,
                    grain=grain,
                    grain_column=grain_col,
                    grain_description=grain_desc,
                )],
            ))

    # Step 7: Save pipeline config
    auto_name = name or classification.name
    auto_id = pipeline_id or classification.publication_id

    config = PipelineConfig(
        pipeline_id=auto_id,
        name=auto_name,
        landing_page=classification.landing_page,
        file_patterns=file_patterns,
        loaded_periods=[latest],
        auto_load=False,
        discovery_mode=classification.discovery_mode,
        url_pattern=classification.url_pattern,
        frequency=classification.frequency,
    )

    save_config(config)

    console.print(Panel(
        f"[green]Pipeline created![/]\n\n"
        f"ID: [bold]{config.pipeline_id}[/]\n"
        f"Name: {config.name}\n"
        f"Files: {len(file_patterns)} pattern(s)\n"
        f"Period: {latest}",
        title="Bootstrap Complete"
    ))


@cli.command()
@click.option('--pipeline', required=True, help='Pipeline ID to scan')
@click.option('--dry-run', is_flag=True, help='Show what would be loaded without loading')
@click.option('--force-scrape', is_flag=True, help='Force landing page scrape even in template mode')
def scan(pipeline: str, dry_run: bool, force_scrape: bool):
    """
    Scan for new periods and load them.

    Uses saved patterns from bootstrap to automatically load new data.
    Discovery mode is determined by the saved pipeline configuration:
    - template: Generate expected period URLs and check which exist
    - discover: Scrape landing page for file links
    - explicit: URLs must be added manually
    """
    config = load_config(pipeline)
    if not config:
        console.print(f"[red]Pipeline '{pipeline}' not found[/]")
        return

    console.print(f"\n[bold blue]Scanning:[/] {config.name}")
    console.print(f"[dim]URL: {config.landing_page}[/]")
    console.print(f"[dim]Discovery mode: {config.discovery_mode}[/]\n")

    # Handle explicit mode
    if config.discovery_mode == 'explicit':
        console.print("[yellow]This pipeline uses explicit mode - URLs must be added manually.[/]")
        return

    # Discover current files based on mode
    files = []
    if config.discovery_mode == 'template' and config.url_pattern and not force_scrape:
        # Template mode: generate period URLs and probe for files
        console.print(f"[dim]Template: {config.url_pattern}[/]")
        files = _discover_via_template(config, console)

        if not files:
            # Fallback to scraping if template discovery fails
            console.print("[dim]Template discovery found no files, falling back to scrape...[/]")
            with console.status("Scraping landing page..."):
                files = scrape_landing_page(config.landing_page)
    else:
        # Discover mode: scrape landing page
        with console.status("Scraping landing page..."):
            files = scrape_landing_page(config.landing_page)

    by_period = _group_files_by_period(files)
    available = sorted([p for p in by_period.keys() if p != 'unknown'], reverse=True)

    # Find new periods
    new_periods = config.get_new_periods(available)

    if not new_periods:
        console.print("[green]No new periods found - up to date![/]")
        return

    console.print(f"[yellow]Found {len(new_periods)} new period(s):[/] {', '.join(sorted(new_periods))}")

    if dry_run:
        console.print("\n[dim]Dry run - no data loaded[/]")
        return

    # Load each new period
    temp_dir = tempfile.mkdtemp()

    for period in sorted(new_periods):
        console.print(f"\n[bold cyan]Loading period: {period}[/]")

        period_files = [item['file'] for item in by_period[period]]

        for fp in config.file_patterns:
            # Find matching file
            matching = [f for f in period_files if re.match(fp.filename_pattern, f.filename, re.IGNORECASE)]

            if not matching:
                console.print(f"  [yellow]No file matching pattern: {fp.filename_pattern}[/]")
                continue

            for f in matching:
                console.print(f"  Processing: {f.filename}")

                with console.status("Downloading..."):
                    local_path = download_file(f.url, temp_dir)

                for sm in fp.sheet_mappings:
                    with console.status(f"Loading {sm.sheet_pattern or 'data'}..."):
                        # Use load_file for CSV, load_sheet for Excel
                        if f.file_type == 'csv' or not sm.sheet_pattern:
                            rows, _, _ = load_file(
                                local_path,
                                sm.table_name,
                                period=period,
                                column_mappings=sm.column_mappings,
                            )
                        else:
                            rows, _, _ = load_sheet(
                                local_path,
                                sm.sheet_pattern,
                                sm.table_name,
                                period=period,
                                column_mappings=sm.column_mappings,
                            )

                    if rows > 0:
                        console.print(f"    [green]{sm.table_name}: {rows} rows[/]")
                        record_load(config.pipeline_id, period, sm.table_name, f.filename, sm.sheet_pattern, rows)
                    else:
                        console.print(f"    [dim]{sm.table_name}: skipped (sheet not found)[/]")

        # Update config with loaded period
        config.add_period(period)
        save_config(config)

    console.print(f"\n[green]Scan complete - loaded {len(new_periods)} period(s)[/]")


@cli.command()
@click.option('--pipeline', required=True, help='Pipeline ID')
@click.option('--from', 'from_period', help='Start period (YYYY-MM)')
@click.option('--to', 'to_period', help='End period (YYYY-MM)')
def backfill(pipeline: str, from_period: Optional[str], to_period: Optional[str]):
    """
    Backfill historical data for a pipeline.

    Loads all periods (or a range) that haven't been loaded yet.
    """
    config = load_config(pipeline)
    if not config:
        console.print(f"[red]Pipeline '{pipeline}' not found[/]")
        return

    console.print(f"\n[bold blue]Backfilling:[/] {config.name}")

    # Discover all files
    with console.status("Scraping landing page..."):
        files = scrape_landing_page(config.landing_page)

    by_period = _group_files_by_period(files)
    available = sorted([p for p in by_period.keys() if p != 'unknown'])

    # Filter by range
    if from_period:
        available = [p for p in available if p >= from_period]
    if to_period:
        available = [p for p in available if p <= to_period]

    # Find unloaded periods
    unloaded = config.get_new_periods(available)

    if not unloaded:
        console.print("[green]All periods already loaded![/]")
        return

    console.print(f"[yellow]Will load {len(unloaded)} period(s)[/]")

    if not Confirm.ask("Continue?", default=True):
        return

    # Load each period (reuse scan logic)
    temp_dir = tempfile.mkdtemp()

    for period in sorted(unloaded):
        console.print(f"\n[bold cyan]Loading period: {period}[/]")

        period_files = [item['file'] for item in by_period[period]]

        for fp in config.file_patterns:
            matching = [f for f in period_files if re.match(fp.filename_pattern, f.filename, re.IGNORECASE)]

            for f in matching:
                with console.status(f"Processing {f.filename}..."):
                    local_path = download_file(f.url, temp_dir)

                for sm in fp.sheet_mappings:
                    # Use load_file for CSV, load_sheet for Excel
                    if f.file_type == 'csv' or not sm.sheet_pattern:
                        rows, _, _ = load_file(
                            local_path,
                            sm.table_name,
                            period=period,
                            column_mappings=sm.column_mappings,
                        )
                    else:
                        rows, _, _ = load_sheet(
                            local_path,
                            sm.sheet_pattern,
                            sm.table_name,
                            period=period,
                            column_mappings=sm.column_mappings,
                        )
                    console.print(f"  {sm.table_name}: {rows} rows")
                    record_load(config.pipeline_id, period, sm.table_name, f.filename, sm.sheet_pattern, rows)

        config.add_period(period)
        save_config(config)

    console.print(f"\n[green]Backfill complete - loaded {len(unloaded)} period(s)[/]")


@cli.command('list')
def list_pipelines():
    """List all registered pipelines."""
    configs = list_configs()

    if not configs:
        console.print("[yellow]No pipelines registered yet[/]")
        console.print("Run: python scripts/pipeline.py bootstrap --url <NHS_URL>")
        return

    table = Table(title="Registered Pipelines")
    table.add_column("ID", style="cyan")
    table.add_column("Name")
    table.add_column("Periods Loaded", justify="right")
    table.add_column("Auto-load")

    for c in configs:
        table.add_row(
            c.pipeline_id,
            c.name,
            str(len(c.loaded_periods)),
            "Yes" if c.auto_load else "No"
        )

    console.print(table)


@cli.command()
@click.option('--pipeline', required=True, help='Pipeline ID')
def history(pipeline: str):
    """Show load history for a pipeline."""
    config = load_config(pipeline)
    if not config:
        console.print(f"[red]Pipeline '{pipeline}' not found[/]")
        return

    records = get_load_history(pipeline)

    if not records:
        console.print(f"[yellow]No load history for '{pipeline}'[/]")
        return

    table = Table(title=f"Load History: {config.name}")
    table.add_column("Period", style="cyan")
    table.add_column("Table")
    table.add_column("Sheet")
    table.add_column("Rows", justify="right")
    table.add_column("Loaded At")

    for r in records:
        table.add_row(
            r['period'],
            r['table_name'],
            r['sheet_name'] or '-',
            str(r['rows_loaded']),
            r['loaded_at'].strftime('%Y-%m-%d %H:%M') if r['loaded_at'] else '-'
        )

    console.print(table)


def _infer_sheet_description(sheet_name: str) -> str:
    """Infer a description from sheet name."""
    name_lower = sheet_name.lower()

    if 'title' in name_lower or 'cover' in name_lower:
        return 'Title/cover page'
    if 'content' in name_lower:
        return 'Table of contents'
    if 'note' in name_lower or 'quality' in name_lower:
        return 'Notes/methodology'
    if 'definition' in name_lower:
        return 'Definitions'

    # Clean up table names
    clean = sheet_name.replace('_', ' ').replace('-', ' ')
    return f"Data: {clean}"


def _extract_name_from_url(url: str) -> str:
    """Extract a reasonable name from URL path."""
    # e.g., /statistical/mi-adhd -> MI ADHD
    path = url.rstrip('/').split('/')[-1]
    name = path.replace('-', ' ').replace('_', ' ').title()
    return name


def _make_filename_pattern(filename: str) -> str:
    """
    Create a regex pattern from a filename that will match similar files.

    e.g., "ADHD-Data-2024-11.xlsx" -> r"ADHD-Data-\d{4}-\d{2}\.xlsx"
    e.g., "adhd_summary_nov25.xlsx" -> r"adhd_summary_[a-z]{3}\d{2}\.xlsx"
    """
    # Escape special regex chars
    pattern = re.escape(filename)

    # Replace date patterns with regex
    # YYYY-MM, YYYY_MM
    pattern = re.sub(r'2\d{3}[-_]\d{2}', r'\\d{4}[-_]\\d{2}', pattern)

    # Abbreviated month + 2-digit year: nov25, aug25, may25
    short_months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                    'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for month in short_months:
        # Match month followed by 2 digits (e.g., nov25)
        pattern = re.sub(f'{month}\\d{{2}}', r'[a-z]{3}\\d{2}', pattern, flags=re.IGNORECASE)

    # Full month names
    months = ['january', 'february', 'march', 'april', 'may', 'june',
              'july', 'august', 'september', 'october', 'november', 'december']
    for month in months:
        pattern = re.sub(month, r'[a-z]+', pattern, flags=re.IGNORECASE)

    return pattern


def _discover_via_template(config, console) -> List:
    """
    Discover files by generating period URLs from template.

    For NHS Digital publications with predictable URLs, this is faster than
    scraping because we can generate URLs directly and check if they exist.

    Returns list of DiscoveredFile objects for periods that exist.
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    # Determine the period range to probe
    # Start from month after last loaded period, or 12 months back if none loaded
    if config.loaded_periods:
        last_loaded = max(config.loaded_periods)
        start = datetime.strptime(last_loaded, '%Y-%m') + relativedelta(months=1)
    else:
        start = datetime.now() - relativedelta(months=12)

    end = datetime.now()

    # Generate period URLs
    period_urls = generate_period_urls(
        config.url_pattern,
        config.landing_page,
        start.strftime('%Y-%m'),
        end.strftime('%Y-%m')
    )

    discovered = []
    with console.status(f"Probing {len(period_urls)} period URLs..."):
        for url in period_urls:
            try:
                # HEAD request to check if period page exists
                resp = requests.head(url, timeout=5, allow_redirects=True)
                if resp.status_code == 200:
                    # Period page exists - scrape it for files
                    files = scrape_landing_page(url)
                    if files:
                        discovered.extend(files)
                        console.print(f"  [green]✓[/] {url.split('/')[-1]}: {len(files)} files")
                    else:
                        console.print(f"  [yellow]○[/] {url.split('/')[-1]}: page exists but no files")
                elif resp.status_code == 404:
                    # Period doesn't exist yet - expected for future months
                    console.print(f"  [dim]✗ {url.split('/')[-1]}: not found[/]")
                else:
                    console.print(f"  [dim]? {url.split('/')[-1]}: HTTP {resp.status_code}[/]")
            except requests.RequestException as e:
                console.print(f"  [red]! {url.split('/')[-1]}: {e}[/]")

    return discovered


if __name__ == '__main__':
    cli()
