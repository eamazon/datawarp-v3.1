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
from typing import List, Optional

import click
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.panel import Panel

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datawarp.discovery import scrape_landing_page, DiscoveredFile
from datawarp.loader import load_sheet, load_file, download_file, get_sheet_names, preview_sheet
from datawarp.pipeline import (
    PipelineConfig, FilePattern, SheetMapping,
    save_config, load_config, list_configs, record_load, get_load_history
)
from datawarp.utils import parse_period, extract_periods_from_files, sanitize_name, make_table_name
from datawarp.storage import get_connection
from datawarp.metadata import detect_grain, enrich_sheet

console = Console()


@click.group()
def cli():
    """DataWarp v3.1 - NHS Data Pipeline"""
    pass


@cli.command()
@click.option('--url', required=True, help='NHS publication landing page URL')
@click.option('--name', help='Pipeline name (auto-generated if not provided)')
@click.option('--id', 'pipeline_id', help='Pipeline ID (auto-generated if not provided)')
@click.option('--enrich', is_flag=True, help='Use LLM to generate semantic column names')
@click.option('--skip-unknown', is_flag=True, default=True, help='Skip sheets with no detected entity')
def bootstrap(url: str, name: Optional[str], pipeline_id: Optional[str], enrich: bool, skip_unknown: bool):
    """
    Bootstrap a new pipeline from an NHS URL.

    Discovers files, groups by period, lets you select what to load,
    then saves the pattern for future scans.

    Use --enrich to call LLM for semantic column names and descriptions.
    """
    console.print(f"\n[bold blue]Discovering files from:[/] {url}\n")

    # Step 1: Scrape URL
    with console.status("Scraping landing page..."):
        files = scrape_landing_page(url)

    if not files:
        console.print("[red]No data files found at this URL[/]")
        return

    console.print(f"[green]Found {len(files)} files[/]\n")

    # Step 2: Group by period
    by_period = extract_periods_from_files([{'filename': f.filename, 'url': f.url, 'file': f} for f in files])

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
            # Show sheets
            sheets = get_sheet_names(local_path)
            console.print(f"  Sheets: {', '.join(sheets)}")

            # Let user select sheets
            if len(sheets) == 1:
                selected_sheets = sheets
            else:
                sheet_selection = Prompt.ask(
                    "  Select sheets (comma-separated, or 'all')",
                    default="all"
                )
                if sheet_selection.lower() == 'all':
                    selected_sheets = sheets
                else:
                    selected_sheets = [s.strip() for s in sheet_selection.split(',')]
                    selected_sheets = [s for s in selected_sheets if s in sheets]

            # Load each selected sheet
            sheet_mappings = []
            auto_name = name or _extract_name_from_url(url)
            auto_id = pipeline_id or sanitize_name(auto_name)

            for sheet in selected_sheets:
                console.print(f"\n  [bold]Loading sheet: {sheet}[/]")

                # Preview and detect grain
                import pandas as pd
                try:
                    preview = pd.read_excel(local_path, sheet_name=sheet, nrows=50)
                except Exception as e:
                    console.print(f"  [red]Error reading sheet: {e}[/]")
                    continue

                if preview.empty:
                    console.print(f"  [dim]Skipping - empty sheet[/]")
                    continue

                # Detect grain (entity type)
                grain_info = detect_grain(preview)
                grain = grain_info['grain']
                grain_col = grain_info['grain_column']
                grain_desc = grain_info['description']

                if skip_unknown and grain == 'unknown':
                    console.print(f"  [dim]Skipping - no entity detected (probably notes/methodology)[/]")
                    continue

                console.print(f"  Grain: [cyan]{grain}[/] ({grain_desc})")
                console.print(f"  Columns: {', '.join(str(c) for c in preview.columns[:5])}{'...' if len(preview.columns) > 5 else ''}")

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
                        grain_hint=grain
                    )
                    table_suffix = enriched['table_name']
                    table_desc = enriched['table_description']
                    col_mappings = enriched['columns']
                    col_descriptions = enriched['descriptions']
                    console.print(f"  [green]LLM suggested: {table_suffix}[/]")
                else:
                    table_suffix = sanitize_name(sheet)
                    table_desc = f"Data from {sheet}"
                    col_mappings = {c: c for c in sanitized_cols}
                    col_descriptions = {}

                table_name = f"tbl_{auto_id}_{table_suffix}"
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

        else:
            # CSV file
            import pandas as pd
            auto_name = name or _extract_name_from_url(url)
            auto_id = pipeline_id or sanitize_name(auto_name)

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
                    grain_hint=grain
                )
                table_suffix = enriched['table_name']
                table_desc = enriched['table_description']
                col_mappings = enriched['columns']
                col_descriptions = enriched['descriptions']
                console.print(f"  [green]LLM suggested: {table_suffix}[/]")
            else:
                table_suffix = sanitize_name(os.path.splitext(f.filename)[0])
                table_desc = f"Data from {f.filename}"
                col_mappings = {c: c for c in sanitized_cols}
                col_descriptions = {}

            table_name = f"tbl_{auto_id}_{table_suffix}"

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
    auto_name = name or _extract_name_from_url(url)
    auto_id = pipeline_id or sanitize_name(auto_name)

    config = PipelineConfig(
        pipeline_id=auto_id,
        name=auto_name,
        landing_page=url,
        file_patterns=file_patterns,
        loaded_periods=[latest],
        auto_load=False,
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
def scan(pipeline: str, dry_run: bool):
    """
    Scan for new periods and load them.

    Uses saved patterns from bootstrap to automatically load new data.
    """
    config = load_config(pipeline)
    if not config:
        console.print(f"[red]Pipeline '{pipeline}' not found[/]")
        return

    console.print(f"\n[bold blue]Scanning:[/] {config.name}")
    console.print(f"[dim]URL: {config.landing_page}[/]\n")

    # Discover current files
    with console.status("Scraping landing page..."):
        files = scrape_landing_page(config.landing_page)

    by_period = extract_periods_from_files([{'filename': f.filename, 'url': f.url, 'file': f} for f in files])
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

    by_period = extract_periods_from_files([{'filename': f.filename, 'url': f.url, 'file': f} for f in files])
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


if __name__ == '__main__':
    cli()
