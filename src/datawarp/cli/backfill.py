"""
Backfill command for DataWarp CLI.

Loads historical data for all (or a range of) periods that haven't been loaded yet.
"""
import tempfile
from typing import Optional

import click
from rich.prompt import Confirm

from datawarp.cli.console import console
from datawarp.cli.helpers import group_files_by_period
from datawarp.cli.file_processor import load_period_files
from datawarp.discovery import scrape_landing_page
from datawarp.pipeline import load_config, save_config
from datawarp.tracking import track_run


@click.command('backfill')
@click.option('--pipeline', required=True, help='Pipeline ID')
@click.option('--from', 'from_period', help='Start period (YYYY-MM)')
@click.option('--to', 'to_period', help='End period (YYYY-MM)')
def backfill_command(pipeline: str, from_period: Optional[str], to_period: Optional[str]):
    """
    Backfill historical data for a pipeline.

    Loads all periods (or a range) that haven't been loaded yet.
    """
    with track_run('backfill', {'pipeline': pipeline, 'from': from_period, 'to': to_period}, pipeline) as tracker:
        _backfill_impl(pipeline, from_period, to_period, tracker)


def _backfill_impl(pipeline: str, from_period: Optional[str], to_period: Optional[str], tracker: dict):
    """Implementation of backfill command."""
    config = load_config(pipeline)
    if not config:
        console.print(f"[red]Pipeline '{pipeline}' not found[/]")
        return

    console.print(f"\n[bold blue]Backfilling:[/] {config.name}")

    # Discover all files
    with console.status("Scraping landing page..."):
        files = scrape_landing_page(config.landing_page)

    by_period = group_files_by_period(files)
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

    # Load each period using shared file processor
    temp_dir = tempfile.mkdtemp()
    total_loaded = 0

    for period in sorted(unloaded):
        console.print(f"\n[bold cyan]Loading period: {period}[/]")

        period_files = [item['file'] for item in by_period[period]]
        results = load_period_files(config, period, period_files, temp_dir, console)

        if results:
            period_rows = sum(rows for _, rows in results)
            total_loaded += period_rows
            config.add_period(period)
            save_config(config)

    # Update tracker
    tracker['periods_loaded'] = list(unloaded)
    tracker['periods_count'] = len(unloaded)
    tracker['total_rows'] = total_loaded

    console.print(f"\n[green]Backfill complete - loaded {len(unloaded)} period(s)[/]")
