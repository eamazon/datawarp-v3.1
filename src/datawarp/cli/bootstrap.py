"""
Bootstrap command - create a new pipeline from an NHS URL.

Discovers files, groups by period, lets user select what to load,
then saves the pattern for future scans.
"""
import os
import tempfile
from typing import List, Optional, Tuple

import click
import pandas as pd
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table
from urllib.parse import unquote

from datawarp.cli.console import console
from datawarp.cli.helpers import group_files_by_period, make_filename_pattern
from datawarp.cli.file_processor import process_data_file
from datawarp.cli.sheet_selector import analyze_sheets, display_sheet_table, select_sheets
from datawarp.discovery import scrape_landing_page, classify_url
from datawarp.loader import download_file, get_sheet_names, load_sheet, load_file
from datawarp.metadata import detect_grain, enrich_sheet
from datawarp.pipeline import PipelineConfig, FilePattern, SheetMapping, save_config, record_load
from datawarp.tracking import track_run
from datawarp.utils import sanitize_name, make_table_name


@click.command('bootstrap')
@click.option('--url', required=True, help='NHS publication landing page URL')
@click.option('--name', help='Pipeline name (auto-generated if not provided)')
@click.option('--id', 'pipeline_id', help='Pipeline ID (auto-generated if not provided)')
@click.option('--enrich', is_flag=True, help='Use LLM to generate semantic column names')
@click.option('--skip-unknown/--no-skip-unknown', default=True, help='Skip sheets with no detected entity')
def bootstrap_command(url: str, name: Optional[str], pipeline_id: Optional[str], enrich: bool, skip_unknown: bool):
    """Bootstrap a new pipeline from an NHS URL."""
    with track_run('bootstrap', {'url': url, 'name': name, 'id': pipeline_id, 'enrich': enrich}, pipeline_id) as tracker:
        _bootstrap_impl(url, name, pipeline_id, enrich, skip_unknown, tracker)


def _classify_and_discover(url: str) -> Tuple[object, List, str]:
    """Classify URL and discover available files."""
    console.print(f"\n[bold blue]Classifying URL...[/]")
    with console.status("Analyzing URL structure..."):
        classification = classify_url(url)

    console.print(Panel(
        f"[bold]{classification.name}[/]\n"
        f"ID: {classification.publication_id}\n"
        f"Source: {classification.source}\n"
        f"Discovery: [bold white]{classification.discovery_mode}[/]\n"
        f"Frequency: {classification.frequency}" +
        (f"\nURL Pattern: {classification.url_pattern}" if classification.url_pattern else "") +
        (f"\n[yellow]Warning: NHS Digital page with NHS England data[/]" if classification.redirects_to_england else ""),
        title="URL Classification"
    ))

    if classification.discovery_mode == 'explicit':
        console.print("[yellow]This publication uses hash-coded URLs that cannot be auto-discovered.[/]")
        console.print("Please provide the exact file URL using --url with a direct file link.")
        return classification, [], None

    scrape_url = classification.original_url if classification.is_period_url else classification.landing_page
    label = "period URL" if classification.is_period_url else "landing page"
    console.print(f"\n[bold blue]Discovering files from {label}:[/] {scrape_url}\n")

    with console.status("Scraping page..."):
        files = scrape_landing_page(scrape_url)

    if not files:
        console.print("[red]No data files found at this URL[/]")
        return classification, [], None

    console.print(f"[green]Found {len(files)} files[/]\n")
    return classification, files, scrape_url


def _select_period_and_files(by_period: dict) -> Tuple[str, List]:
    """Display periods and let user select period and files."""
    periods = sorted([p for p in by_period.keys() if p != 'unknown'], reverse=True)
    if not periods:
        console.print("[red]Could not detect periods from filenames[/]")
        return None, []

    table = Table(title="Available Periods")
    table.add_column("Period", style="bold white")
    table.add_column("Files", justify="right")
    for period in periods[:10]:
        table.add_row(period, str(len(by_period[period])))
    if len(periods) > 10:
        table.add_row("...", f"({len(periods) - 10} more)")
    console.print(table)

    latest = periods[0]
    console.print(f"\n[bold]Latest period: {latest}[/] ({len(by_period[latest])} files)")
    if not Confirm.ask("Bootstrap from this period?", default=True):
        latest = Prompt.ask("Enter period to bootstrap from", choices=periods)

    period_files = [item['file'] for item in by_period[latest]]
    console.print(f"\n[bold]Files in {latest}:[/]")
    for i, f in enumerate(period_files, 1):
        console.print(f"  {i}. {unquote(f.filename)} ({f.file_type})")

    if len(period_files) == 1:
        return latest, period_files

    selection = Prompt.ask("\nSelect files (comma-separated numbers, or 'all')", default="all")
    if selection.lower() == 'all':
        return latest, period_files
    indices = [int(x.strip()) - 1 for x in selection.split(',')]
    return latest, [period_files[i] for i in indices if 0 <= i < len(period_files)]


def _load_sheets(selected: List[dict], local_path: str, period: str, auto_id: str, enrich: bool, filename: str) -> List[SheetMapping]:
    """Load selected sheets to database and return mappings."""
    mappings = []
    for sp in selected:
        sheet, df, grain_info = sp['name'], sp['df'], sp['grain_info']
        grain, grain_col, grain_desc = grain_info['grain'], grain_info['grain_column'], grain_info['description']

        console.print(f"\n  [bold]Loading: {sheet}[/] ({sp['rows']} rows, {grain})")
        sanitized_cols = [sanitize_name(str(c)) for c in df.columns if not str(c).lower().startswith('unnamed')]

        if enrich:
            console.print("  [yellow]Enriching with LLM...[/]")
            enriched = enrich_sheet(
                sheet_name=sheet, columns=sanitized_cols, sample_rows=df.head(3).to_dict('records'),
                publication_hint=auto_id, grain_hint=grain, pipeline_id=auto_id, source_file=local_path
            )
            table_name = f"tbl_{sanitize_name(enriched['table_name'])}"[:63]
            table_desc, col_mappings, col_descriptions = enriched['table_description'], enriched['columns'], enriched['descriptions']
            console.print(f"  [green]LLM suggested: {table_name}[/]")
        else:
            table_name = make_table_name(auto_id, sanitize_name(sheet))
            table_desc, col_mappings, col_descriptions = f"Data from {sheet}", {c: c for c in sanitized_cols}, {}

        console.print(f"  [dim]Table: staging.{table_name}[/]")
        with console.status("Loading to database..."):
            rows, learned_mappings, col_types = load_sheet(local_path, sheet, table_name, period=period, column_mappings=col_mappings)

        if rows == 0:
            console.print("  [dim]Skipped (no data)[/]")
            continue

        console.print(f"  [green]Loaded {rows} rows[/]")
        record_load(auto_id, period, table_name, filename, sheet, rows)
        mappings.append(SheetMapping(
            sheet_pattern=sheet, table_name=table_name, table_description=table_desc,
            column_mappings=learned_mappings, column_descriptions=col_descriptions,
            column_types=col_types, grain=grain, grain_column=grain_col, grain_description=grain_desc,
        ))
    return mappings


def _process_excel(local_path: str, f, period: str, auto_id: str, enrich: bool, skip_unknown: bool) -> List[SheetMapping]:
    """Process Excel file with sheet analysis and selection."""
    sheets = get_sheet_names(local_path)
    console.print(f"\n  [bold]Analyzing {len(sheets)} sheets...[/]")
    previews = analyze_sheets(local_path, sheets)
    display_sheet_table(previews, f.filename)
    selected = select_sheets(previews, skip_unknown)
    if not selected:
        console.print("  [yellow]No valid sheets selected[/]")
        return []
    return _load_sheets(selected, local_path, period, auto_id, enrich, f.filename)


def _process_csv(local_path: str, f, period: str, auto_id: str, enrich: bool) -> List[SheetMapping]:
    """Process CSV file and return sheet mappings."""
    try:
        preview = pd.read_csv(local_path, nrows=50)
    except Exception as e:
        console.print(f"  [red]Error reading CSV: {e}[/]")
        return []

    grain_info = detect_grain(preview)
    grain, grain_col, grain_desc = grain_info['grain'], grain_info['grain_column'], grain_info['description']
    console.print(f"  Grain: [bold white]{grain}[/] ({grain_desc})")
    sanitized_cols = [sanitize_name(str(c)) for c in preview.columns if not str(c).lower().startswith('unnamed')]

    if enrich:
        console.print("  [yellow]Enriching with LLM...[/]")
        enriched = enrich_sheet(
            sheet_name=os.path.splitext(f.filename)[0], columns=sanitized_cols, sample_rows=preview.head(3).to_dict('records'),
            publication_hint=auto_id, grain_hint=grain, pipeline_id=auto_id, source_file=local_path
        )
        table_name = f"tbl_{sanitize_name(enriched['table_name'])}"[:63]
        table_desc, col_mappings, col_descriptions = enriched['table_description'], enriched['columns'], enriched['descriptions']
        console.print(f"  [green]LLM suggested: {table_name}[/]")
    else:
        table_name = make_table_name(auto_id, sanitize_name(os.path.splitext(f.filename)[0]))
        table_desc, col_mappings, col_descriptions = f"Data from {unquote(f.filename)}", {c: c for c in sanitized_cols}, {}

    with console.status("Loading to database..."):
        rows, learned_mappings, col_types = load_file(local_path, table_name, period=period, column_mappings=col_mappings)

    console.print(f"  [green]Loaded {rows} rows to staging.{table_name}[/]")
    record_load(auto_id, period, table_name, f.filename, None, rows)
    return [SheetMapping(
        sheet_pattern='', table_name=table_name, table_description=table_desc, column_mappings=learned_mappings,
        column_descriptions=col_descriptions, column_types=col_types, grain=grain, grain_column=grain_col, grain_description=grain_desc,
    )]


def _save_pipeline(classification, auto_name: str, auto_id: str, file_patterns: List, latest: str, tracker: dict):
    """Save pipeline configuration and update tracker."""
    config = PipelineConfig(
        pipeline_id=auto_id, name=auto_name, landing_page=classification.landing_page,
        file_patterns=file_patterns, loaded_periods=[latest], auto_load=False,
        discovery_mode=classification.discovery_mode, url_pattern=classification.url_pattern, frequency=classification.frequency,
    )
    save_config(config)
    tracker['pipeline_id'] = config.pipeline_id
    tracker['period'] = latest
    tracker['files_processed'] = len(file_patterns)
    tracker['tables_created'] = [sm.table_name for fp in file_patterns for sm in fp.sheet_mappings]

    console.print(Panel(
        f"[green]Pipeline created![/]\n\nID: [bold]{config.pipeline_id}[/]\nName: {config.name}\n"
        f"Files: {len(file_patterns)} pattern(s)\nPeriod: {latest}",
        title="Bootstrap Complete"
    ))


def _bootstrap_impl(url: str, name: Optional[str], pipeline_id: Optional[str], enrich: bool, skip_unknown: bool, tracker: dict):
    """Main bootstrap implementation."""
    classification, files, _ = _classify_and_discover(url)
    if not files:
        return

    by_period = group_files_by_period(files)
    latest, selected_files = _select_period_and_files(by_period)
    if not selected_files:
        console.print("[red]No files selected[/]")
        return

    auto_name, auto_id = name or classification.name, pipeline_id or classification.publication_id
    file_patterns, temp_dir = [], tempfile.mkdtemp()

    for f in selected_files:
        console.print(f"\n[bold cyan]Processing: {unquote(f.filename)}[/]")
        with console.status("Downloading..."):
            local_path = download_file(f.url, temp_dir)

        if f.file_type in ['xlsx', 'xls']:
            mappings = _process_excel(local_path, f, latest, auto_id, enrich, skip_unknown)
        elif f.file_type == 'zip':
            results = process_data_file(local_path, f.filename, 'zip', latest, auto_id, enrich, console)
            mappings = [m for m, _ in results]
            for m, rows in results:
                record_load(auto_id, latest, m.table_name, f.filename, m.sheet_pattern or f.filename, rows)
        else:
            mappings = _process_csv(local_path, f, latest, auto_id, enrich)

        if mappings:
            file_patterns.append(FilePattern(filename_pattern=make_filename_pattern(f.filename), file_types=[f.file_type], sheet_mappings=mappings))

    _save_pipeline(classification, auto_name, auto_id, file_patterns, latest, tracker)
