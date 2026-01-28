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
from datawarp.pipeline import PipelineConfig, FilePattern, SheetMapping, save_config, record_load, load_config
from datawarp.tracking import track_run
from datawarp.storage import get_connection
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
    console.print(f"\n[info]Classifying URL...[/]")
    with console.status("Analyzing URL structure..."):
        classification = classify_url(url)

    console.print(Panel(
        f"[bold]{classification.name}[/]\n"
        f"ID: {classification.publication_id}\n"
        f"Source: {classification.source}\n"
        f"Discovery: [blue]{classification.discovery_mode}[/]\n"
        f"Frequency: {classification.frequency}" +
        (f"\nURL Pattern: {classification.url_pattern}" if classification.url_pattern else "") +
        (f"\n[warning]Warning: NHS Digital page with NHS England data[/]" if classification.redirects_to_england else ""),
        title="URL Classification"
    ))

    if classification.discovery_mode == 'explicit':
        console.print("[warning]This publication uses hash-coded URLs that cannot be auto-discovered.[/]")
        console.print("Please provide the exact file URL using --url with a direct file link.")
        return classification, [], None

    scrape_url = classification.original_url if classification.is_period_url else classification.landing_page
    label = "period URL" if classification.is_period_url else "landing page"
    console.print(f"\n[info]Discovering files from {label}:[/] {scrape_url}\n")

    with console.status("Scraping page..."):
        files = scrape_landing_page(scrape_url)

    if not files:
        console.print("[error]No data files found at this URL[/]")
        return classification, [], None

    console.print(f"[success]Found {len(files)} files[/]\n")
    return classification, files, scrape_url


def _select_period_and_files(by_period: dict) -> Tuple[str, List]:
    """Display periods and let user select period and files."""
    periods = sorted([p for p in by_period.keys() if p != 'unknown'], reverse=True)
    if not periods:
        console.print("[error]Could not detect periods from filenames[/]")
        return None, []

    table = Table(title="Available Periods", header_style="bold blue")
    table.add_column("Period", style="blue")
    table.add_column("Files", justify="right", style="blue")
    for period in periods[:10]:
        table.add_row(period, str(len(by_period[period])))
    if len(periods) > 10:
        table.add_row("...", f"({len(periods) - 10} more)")
    console.print(table)

    # Calculate total files across all displayed periods
    total_files = sum(len(by_period[p]) for p in periods)

    # If few periods (e.g., from a period URL), offer to load all
    if len(periods) <= 3:
        console.print(f"\n[bold]Total: {total_files} files across {len(periods)} period(s)[/]")
        choice = Prompt.ask(
            "Load from which period?",
            choices=periods + ['all'],
            default='all'
        )
        if choice == 'all':
            # Return all files with the latest period as reference
            all_files = []
            for p in periods:
                all_files.extend([item['file'] for item in by_period[p]])
            console.print(f"\n[bold]All {len(all_files)} files:[/]")
            for i, f in enumerate(all_files, 1):
                console.print(f"  {i}. {unquote(f.filename)} ({f.file_type})")
            return periods[0], all_files  # Use latest period as reference

        latest = choice
    else:
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
            console.print("  [warning]Enriching with LLM...[/]")
            enriched = enrich_sheet(
                sheet_name=sheet, columns=sanitized_cols, sample_rows=df.head(3).to_dict('records'),
                publication_hint=auto_id, grain_hint=grain, pipeline_id=auto_id, source_file=local_path
            )
            table_name = f"tbl_{sanitize_name(enriched['table_name'])}"[:63]
            table_desc, col_mappings, col_descriptions = enriched['table_description'], enriched['columns'], enriched['descriptions']
            console.print(f"  [success]LLM suggested: {table_name}[/]")
        else:
            table_name = make_table_name(auto_id, sanitize_name(sheet))
            table_desc, col_mappings, col_descriptions = f"Data from {sheet}", {c: c for c in sanitized_cols}, {}

        console.print(f"  [muted]Table: staging.{table_name}[/]")
        with console.status("Loading to database..."):
            rows, learned_mappings, col_types = load_sheet(local_path, sheet, table_name, period=period, column_mappings=col_mappings)

        if rows == 0:
            console.print("  [muted]Skipped (no data)[/]")
            continue

        console.print(f"  [success]Loaded {rows} rows[/]")
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
        console.print("  [warning]No valid sheets selected[/]")
        return []
    return _load_sheets(selected, local_path, period, auto_id, enrich, f.filename)


def _process_csv(local_path: str, f, period: str, auto_id: str, enrich: bool) -> List[SheetMapping]:
    """Process CSV file and return sheet mappings."""
    try:
        preview = pd.read_csv(local_path, nrows=50)
    except Exception as e:
        console.print(f"  [error]Error reading CSV: {e}[/]")
        return []

    grain_info = detect_grain(preview)
    grain, grain_col, grain_desc = grain_info['grain'], grain_info['grain_column'], grain_info['description']
    console.print(f"  Grain: [bold white]{grain}[/] ({grain_desc})")
    sanitized_cols = [sanitize_name(str(c)) for c in preview.columns if not str(c).lower().startswith('unnamed')]

    if enrich:
        console.print("  [warning]Enriching with LLM...[/]")
        enriched = enrich_sheet(
            sheet_name=os.path.splitext(f.filename)[0], columns=sanitized_cols, sample_rows=preview.head(3).to_dict('records'),
            publication_hint=auto_id, grain_hint=grain, pipeline_id=auto_id, source_file=local_path
        )
        table_name = f"tbl_{sanitize_name(enriched['table_name'])}"[:63]
        table_desc, col_mappings, col_descriptions = enriched['table_description'], enriched['columns'], enriched['descriptions']
        console.print(f"  [success]LLM suggested: {table_name}[/]")
    else:
        table_name = make_table_name(auto_id, sanitize_name(os.path.splitext(f.filename)[0]))
        table_desc, col_mappings, col_descriptions = f"Data from {unquote(f.filename)}", {c: c for c in sanitized_cols}, {}

    with console.status("Loading to database..."):
        rows, learned_mappings, col_types = load_file(local_path, table_name, period=period, column_mappings=col_mappings)

    console.print(f"  [success]Loaded {rows} rows to staging.{table_name}[/]")
    record_load(auto_id, period, table_name, f.filename, None, rows)
    return [SheetMapping(
        sheet_pattern='', table_name=table_name, table_description=table_desc, column_mappings=learned_mappings,
        column_descriptions=col_descriptions, column_types=col_types, grain=grain, grain_column=grain_col, grain_description=grain_desc,
    )]


def _save_pipeline(classification, auto_name: str, auto_id: str, file_patterns: List, loaded_periods: List[str], tracker: dict, is_update: bool = False, tables_before_load: set = None):
    """Save pipeline configuration and update tracker."""
    config = PipelineConfig(
        pipeline_id=auto_id, name=auto_name, landing_page=classification.landing_page,
        file_patterns=file_patterns, loaded_periods=sorted(loaded_periods), auto_load=False,
        discovery_mode=classification.discovery_mode, url_pattern=classification.url_pattern, frequency=classification.frequency,
    )
    save_config(config)

    # Collect all unique table names from this run
    all_tables = []
    seen = set()
    for fp in file_patterns:
        for sm in fp.sheet_mappings:
            if sm.table_name not in seen:
                all_tables.append(sm.table_name)
                seen.add(sm.table_name)

    # Categorize tables based on what existed BEFORE loading
    tables_before_load = tables_before_load or set()
    tables_created = [t for t in all_tables if t not in tables_before_load]
    tables_updated = [t for t in all_tables if t in tables_before_load]

    tracker['pipeline_id'] = config.pipeline_id
    tracker['periods'] = loaded_periods
    tracker['files_processed'] = len(file_patterns)
    tracker['tables_created'] = tables_created
    tracker['tables_updated'] = tables_updated

    # Build summary
    summary_parts = []
    if tables_created:
        summary_parts.append(f"[bold]Tables created:[/]\n" + "\n".join(f"  - staging.{t}" for t in tables_created))
    if tables_updated:
        summary_parts.append(f"[bold]Tables updated:[/]\n" + "\n".join(f"  - staging.{t}" for t in tables_updated))
    if not summary_parts:
        summary_parts.append("  (no tables)")

    tables_summary = "\n\n".join(summary_parts)
    action = "updated" if is_update else "created"
    periods_str = ", ".join(sorted(loaded_periods))

    console.print(Panel(
        f"[blue]Pipeline {action}![/]\n\n"
        f"ID: [bold]{config.pipeline_id}[/]\n"
        f"Name: {config.name}\n"
        f"Periods: {periods_str}\n\n"
        f"{tables_summary}",
        title="Bootstrap Complete"
    ))


def _bootstrap_impl(url: str, name: Optional[str], pipeline_id: Optional[str], enrich: bool, skip_unknown: bool, tracker: dict):
    """Main bootstrap implementation."""
    classification, files, _ = _classify_and_discover(url)
    if not files:
        return

    by_period = group_files_by_period(files)

    # Check if pipeline already exists
    auto_id = pipeline_id or classification.publication_id
    existing = load_config(auto_id)
    is_update = existing is not None

    if existing:
        # Pipeline exists - show status and offer options
        available_periods = sorted([p for p in by_period.keys() if p != 'unknown'], reverse=True)
        new_periods = existing.get_new_periods(available_periods)

        console.print(Panel(
            f"[bold]Pipeline already exists![/]\n\n"
            f"ID: {existing.pipeline_id}\n"
            f"Name: {existing.name}\n"
            f"Periods loaded: {len(existing.loaded_periods)}\n"
            f"Latest loaded: {max(existing.loaded_periods) if existing.loaded_periods else 'none'}\n"
            f"New periods available: {len(new_periods)}",
            title="Existing Pipeline"
        ))

        if new_periods:
            console.print(f"\n[blue]New periods: {', '.join(sorted(new_periods)[:5])}{'...' if len(new_periods) > 5 else ''}[/]")
            console.print("\nTo load new periods, run:")
            console.print(f"  [bold]python scripts/pipeline.py scan --pipeline {auto_id}[/]")
        else:
            console.print("\n[blue]All available periods already loaded![/]")

        if not Confirm.ask("\nRe-bootstrap anyway? (will replace existing config)", default=False):
            return
        console.print("")  # Blank line before continuing

    latest, selected_files = _select_period_and_files(by_period)
    if not selected_files:
        console.print("[error]No files selected[/]")
        return

    auto_name, auto_id = name or classification.name, pipeline_id or classification.publication_id
    file_patterns, temp_dir = [], tempfile.mkdtemp()

    # Check which tables exist BEFORE loading (to distinguish created vs updated)
    tables_before_load = set()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'staging'")
            tables_before_load = {row[0] for row in cur.fetchall()}

    loaded_periods = set()
    for f in selected_files:
        # Use file's own period if available, otherwise fall back to latest
        file_period = f.period or latest
        loaded_periods.add(file_period)

        console.print(f"\n[highlight]Processing: {unquote(f.filename)}[/] (period: {file_period})")
        with console.status("Downloading..."):
            local_path = download_file(f.url, temp_dir)

        if f.file_type in ['xlsx', 'xls']:
            mappings = _process_excel(local_path, f, file_period, auto_id, enrich, skip_unknown)
        elif f.file_type == 'zip':
            results = process_data_file(local_path, f.filename, 'zip', file_period, auto_id, enrich, console)
            mappings = [m for m, _ in results]
            for m, rows in results:
                record_load(auto_id, file_period, m.table_name, f.filename, m.sheet_pattern or f.filename, rows)
        else:
            mappings = _process_csv(local_path, f, file_period, auto_id, enrich)

        if mappings:
            file_patterns.append(FilePattern(filename_pattern=make_filename_pattern(f.filename), file_types=[f.file_type], sheet_mappings=mappings))

    _save_pipeline(classification, auto_name, auto_id, file_patterns, list(loaded_periods), tracker, is_update, tables_before_load)
