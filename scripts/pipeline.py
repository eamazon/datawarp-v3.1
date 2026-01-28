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
import sys

import click

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datawarp.cli.bootstrap import bootstrap_command
from datawarp.cli.scan import scan_command
from datawarp.cli.backfill import backfill_command
from datawarp.cli.list_history import list_command, history_command
from datawarp.cli.enrich import enrich_command
from datawarp.cli.add_sheet import add_sheet_command


@click.group()
def cli():
    """DataWarp v3.1 - NHS Data Pipeline"""
    pass


cli.add_command(bootstrap_command, name='bootstrap')
cli.add_command(scan_command, name='scan')
cli.add_command(backfill_command, name='backfill')
cli.add_command(enrich_command, name='enrich')
cli.add_command(add_sheet_command, name='add-sheet')
cli.add_command(list_command, name='list')
cli.add_command(history_command, name='history')


if __name__ == '__main__':
    cli()
