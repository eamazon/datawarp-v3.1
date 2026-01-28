"""
List and history CLI commands for DataWarp.

Commands for viewing pipeline configurations and load history.
"""
import click
from rich.table import Table

from datawarp.cli.console import console
from datawarp.tracking import track_run
from datawarp.pipeline import load_config, list_configs, get_load_history


@click.command('list')
def list_command():
    """List all registered pipelines."""
    with track_run('list', {}) as tracker:
        configs = list_configs()
        tracker['pipeline_count'] = len(configs)

        if not configs:
            console.print("[yellow]No pipelines registered yet[/]")
            console.print("Run: python scripts/pipeline.py bootstrap --url <NHS_URL>")
            return

        table = Table(title="Registered Pipelines")
        table.add_column("ID", style="bold white")
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


@click.command('history')
@click.option('--pipeline', required=True, help='Pipeline ID')
def history_command(pipeline: str):
    """Show load history for a pipeline."""
    with track_run('history', {'pipeline': pipeline}, pipeline) as tracker:
        config = load_config(pipeline)
        if not config:
            console.print(f"[red]Pipeline '{pipeline}' not found[/]")
            return

        records = get_load_history(pipeline)
        tracker['record_count'] = len(records)

        if not records:
            console.print(f"[yellow]No load history for '{pipeline}'[/]")
            return

        table = Table(title=f"Load History: {config.name}")
        table.add_column("Period", style="bold white")
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
