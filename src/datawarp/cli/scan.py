"""
Scan command - find and load new periods for a pipeline.
"""
import tempfile
from datetime import datetime
from typing import List

import click
import requests
from dateutil.relativedelta import relativedelta

from datawarp.cli.console import console
from datawarp.cli.helpers import group_files_by_period
from datawarp.cli.file_processor import load_period_files
from datawarp.discovery import scrape_landing_page, generate_period_urls
from datawarp.pipeline import load_config, save_config
from datawarp.tracking import track_run


@click.command('scan')
@click.option('--pipeline', required=True, help='Pipeline ID to scan')
@click.option('--dry-run', is_flag=True, help='Show what would be loaded without loading')
@click.option('--force-scrape', is_flag=True, help='Force landing page scrape even in template mode')
def scan_command(pipeline: str, dry_run: bool, force_scrape: bool):
    """
    Scan for new periods and load them.

    Uses saved patterns from bootstrap to automatically load new data.
    Discovery mode is determined by the saved pipeline configuration:
    - template: Generate expected period URLs and check which exist
    - discover: Scrape landing page for file links
    - explicit: URLs must be added manually
    """
    with track_run('scan', {'pipeline': pipeline, 'dry_run': dry_run}, pipeline) as tracker:
        _scan_impl(pipeline, dry_run, force_scrape, tracker)


def _scan_impl(pipeline: str, dry_run: bool, force_scrape: bool, tracker: dict):
    """Implementation of scan command."""
    config = load_config(pipeline)
    if not config:
        console.print(f"[error]Pipeline '{pipeline}' not found[/]")
        return

    console.print(f"\n[info]Scanning:[/] {config.name}")
    console.print(f"[muted]URL: {config.landing_page}[/]")
    console.print(f"[muted]Discovery mode: {config.discovery_mode}[/]\n")

    # Handle explicit mode
    if config.discovery_mode == 'explicit':
        console.print("[warning]This pipeline uses explicit mode - URLs must be added manually.[/]")
        return

    # Discover current files based on mode
    files = []
    if config.discovery_mode == 'template' and config.url_pattern and not force_scrape:
        # Template mode: generate period URLs and probe for files
        console.print(f"[muted]Template: {config.url_pattern}[/]")
        files = _discover_via_template(config)

        if not files:
            # Fallback to scraping if template discovery fails
            console.print("[muted]Template discovery found no files, falling back to scrape...[/]")
            with console.status("Scraping landing page..."):
                files = scrape_landing_page(config.landing_page)
    else:
        # Discover mode: scrape landing page
        with console.status("Scraping landing page..."):
            files = scrape_landing_page(config.landing_page)

    by_period = group_files_by_period(files)
    available = sorted([p for p in by_period.keys() if p != 'unknown'], reverse=True)

    # Find new periods
    new_periods = config.get_new_periods(available)

    if not new_periods:
        console.print("[success]No new periods found - up to date![/]")
        return

    console.print(f"[warning]Found {len(new_periods)} new period(s):[/] {', '.join(sorted(new_periods))}")

    if dry_run:
        console.print("\n[muted]Dry run - no data loaded[/]")
        return

    # Load each new period
    temp_dir = tempfile.mkdtemp()

    for period in sorted(new_periods):
        console.print(f"\n[highlight]Loading period: {period}[/]")

        period_files = [item['file'] for item in by_period[period]]

        # Use shared load_period_files from file_processor
        results = load_period_files(config, period, period_files, temp_dir, console)

        if results:
            # Update config with loaded period
            config.add_period(period)
            save_config(config)

    # Update tracker
    tracker['periods_loaded'] = list(new_periods)
    tracker['periods_count'] = len(new_periods)

    console.print(f"\n[success]Scan complete - loaded {len(new_periods)} period(s)[/]")


def _discover_via_template(config) -> List:
    """
    Discover files by generating period URLs from template.

    For NHS Digital publications with predictable URLs, this is faster than
    scraping because we can generate URLs directly and check if they exist.

    Returns list of DiscoveredFile objects for periods that exist.
    """
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
                        console.print(f"  [success]o[/] {url.split('/')[-1]}: {len(files)} files")
                    else:
                        console.print(f"  [warning]o[/] {url.split('/')[-1]}: page exists but no files")
                elif resp.status_code == 404:
                    # Period doesn't exist yet - expected for future months
                    console.print(f"  [muted]x {url.split('/')[-1]}: not found[/]")
                else:
                    console.print(f"  [muted]? {url.split('/')[-1]}: HTTP {resp.status_code}[/]")
            except requests.RequestException as e:
                console.print(f"  [error]! {url.split('/')[-1]}: {e}[/]")

    return discovered
