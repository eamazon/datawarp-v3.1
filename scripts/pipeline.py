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

console = Console()


@click.group()
def cli():
    """DataWarp v3.1 - NHS Data Pipeline"""
    pass


@cli.command()
@click.option('--url', required=True, help='NHS publication landing page URL')
@click.option('--name', help='Pipeline name (auto-generated if not provided)')
@click.option('--id', 'pipeline_id', help='Pipeline ID (auto-generated if not provided)')
def bootstrap(url: str, name: Optional[str], pipeline_id: Optional[str]):
    """
    Bootstrap a new pipeline from an NHS URL.

    Discovers files, groups by period, lets you select what to load,
    then saves the pattern for future scans.
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
            for sheet in selected_sheets:
                console.print(f"\n  [bold]Loading sheet: {sheet}[/]")

                # Preview
                preview = preview_sheet(local_path, sheet)
                console.print(f"  Columns: {', '.join(preview.columns[:5])}{'...' if len(preview.columns) > 5 else ''}")
                console.print(f"  Preview rows: {len(preview)}")

                # Generate table name
                auto_name = name or _extract_name_from_url(url)
                auto_id = pipeline_id or sanitize_name(auto_name)
                table_name = make_table_name(auto_id, sheet)

                console.print(f"  [dim]Table: staging.{table_name}[/]")

                # Load data
                with console.status("Loading to database..."):
                    rows, col_mappings, col_types = load_sheet(
                        local_path, sheet, table_name,
                        period=latest
                    )

                console.print(f"  [green]Loaded {rows} rows[/]")

                # Record load
                record_load(auto_id, latest, table_name, f.filename, sheet, rows)

                # Save sheet mapping for future use
                sheet_mappings.append(SheetMapping(
                    sheet_pattern=sheet,
                    table_name=table_name,
                    column_mappings=col_mappings,
                    column_types=col_types,
                ))

            # Create file pattern
            file_patterns.append(FilePattern(
                filename_pattern=_make_filename_pattern(f.filename),
                file_types=[f.file_type],
                sheet_mappings=sheet_mappings,
            ))

        else:
            # CSV file
            auto_name = name or _extract_name_from_url(url)
            auto_id = pipeline_id or sanitize_name(auto_name)
            table_name = make_table_name(auto_id, os.path.splitext(f.filename)[0])

            with console.status("Loading to database..."):
                rows, col_mappings, col_types = load_file(
                    local_path, table_name,
                    period=latest
                )

            console.print(f"  [green]Loaded {rows} rows to staging.{table_name}[/]")
            record_load(auto_id, latest, table_name, f.filename, None, rows)

            file_patterns.append(FilePattern(
                filename_pattern=_make_filename_pattern(f.filename),
                file_types=[f.file_type],
                sheet_mappings=[SheetMapping(
                    sheet_pattern='',
                    table_name=table_name,
                    column_mappings=col_mappings,
                    column_types=col_types,
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
                        rows, _, _ = load_sheet(
                            local_path,
                            sm.sheet_pattern if sm.sheet_pattern else None,
                            sm.table_name,
                            period=period,
                            column_mappings=sm.column_mappings,
                        )

                    console.print(f"    [green]{sm.table_name}: {rows} rows[/]")
                    record_load(config.pipeline_id, period, sm.table_name, f.filename, sm.sheet_pattern, rows)

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
                    rows, _, _ = load_sheet(
                        local_path,
                        sm.sheet_pattern if sm.sheet_pattern else None,
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
    """
    # Escape special regex chars
    pattern = re.escape(filename)

    # Replace date patterns with regex
    # YYYY-MM, YYYY_MM
    pattern = re.sub(r'\\d{4}[-_]\\d{2}', r'\\d{4}[-_]\\d{2}', pattern)
    pattern = re.sub(r'2\d{3}[-_]\d{2}', r'\\d{4}[-_]\\d{2}', pattern)

    # Month names
    months = ['january', 'february', 'march', 'april', 'may', 'june',
              'july', 'august', 'september', 'october', 'november', 'december']
    for month in months:
        pattern = re.sub(month, r'[a-z]+', pattern, flags=re.IGNORECASE)

    return pattern


if __name__ == '__main__':
    cli()
