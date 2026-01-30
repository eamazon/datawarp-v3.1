"""
Reset command - clear loaded data while preserving enrichment mappings.

Clears staging tables and load history, but keeps the expensive LLM-generated
table names and column mappings in the pipeline config.

Use --delete to completely remove the pipeline including config and enrichment logs.
"""
import click
from rich.panel import Panel
from rich.prompt import Confirm
from rich.table import Table

from datawarp.cli.console import console
from datawarp.pipeline import load_config, save_config
from datawarp.storage import get_connection


@click.command('reset')
@click.option('--pipeline', '-p', required=True, help='Pipeline ID to reset')
@click.option('--yes', '-y', is_flag=True, help='Skip confirmation prompt')
@click.option('--delete', is_flag=True, help='Completely remove pipeline including config and enrichment logs')
def reset_command(pipeline: str, yes: bool, delete: bool):
    """Clear loaded data while keeping enrichment mappings.

    Use --delete to completely remove the pipeline including enrichment."""
    config = load_config(pipeline)
    if not config:
        console.print(f"[error]Pipeline '{pipeline}' not found[/]")
        return

    # Collect table names from config
    tables = []
    for fp in config.file_patterns:
        for sm in fp.sheet_mappings:
            if sm.table_name and sm.table_name not in tables:
                tables.append(sm.table_name)

    # Show what will be affected
    console.print(Panel(
        f"[bold]Pipeline:[/] {config.pipeline_id}\n"
        f"[bold]Name:[/] {config.name}\n"
        f"[bold]Periods loaded:[/] {len(config.loaded_periods)}\n"
        f"[bold]Tables:[/] {len(tables)}",
        title="Reset Pipeline Data"
    ))

    if tables:
        tbl = Table(title="Tables to clear", header_style="bold")
        tbl.add_column("Table", style="blue")
        tbl.add_column("Status")
        with get_connection() as conn:
            with conn.cursor() as cur:
                for t in tables:
                    cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='staging' AND table_name=%s", (t,))
                    exists = cur.fetchone()[0] > 0
                    if exists:
                        cur.execute(f"SELECT COUNT(*) FROM staging.{t}")
                        rows = cur.fetchone()[0]
                        tbl.add_row(f"staging.{t}", f"{rows} rows")
                    else:
                        tbl.add_row(f"staging.{t}", "[muted]not created[/]")
        console.print(tbl)

    console.print("\n[bold]This will:[/]")
    console.print("  1. Drop staging tables listed above")
    console.print("  2. Clear load history for this pipeline")
    if delete:
        console.print("  3. Delete enrichment logs")
        console.print("  4. Delete CLI run history")
        console.print("  5. Delete pipeline config")
        console.print("\n[warning]This is permanent - enrichment will need to be regenerated![/]")
    else:
        console.print("  3. Reset loaded_periods to empty")
        console.print("\n[success]Preserved:[/] table names, column mappings, descriptions (enrichment)")

    if not yes and not Confirm.ask("\nProceed?", default=False):
        console.print("[muted]Cancelled[/]")
        return

    # Execute reset
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Drop staging tables
            for t in tables:
                cur.execute(f"DROP TABLE IF EXISTS staging.{t}")
                console.print(f"  [muted]Dropped staging.{t}[/]")

            # Clear load history
            cur.execute("DELETE FROM datawarp.tbl_load_history WHERE pipeline_id = %s", (pipeline,))
            console.print(f"  [muted]Cleared load history[/]")

            if delete:
                # Delete enrichment logs
                cur.execute("DELETE FROM datawarp.tbl_enrichment_log WHERE pipeline_id = %s", (pipeline,))
                console.print(f"  [muted]Deleted enrichment logs[/]")

                # Delete CLI run history
                cur.execute("DELETE FROM datawarp.tbl_cli_runs WHERE pipeline_id = %s", (pipeline,))
                console.print(f"  [muted]Deleted CLI run history[/]")

                # Delete pipeline config
                cur.execute("DELETE FROM datawarp.tbl_pipeline_configs WHERE pipeline_id = %s", (pipeline,))
                console.print(f"  [muted]Deleted pipeline config[/]")

                console.print(f"\n[success]Pipeline '{pipeline}' completely removed![/]")
            else:
                # Reset loaded_periods in config (keeps everything else)
                config.loaded_periods = []
                save_config(config)
                console.print(f"  [muted]Reset loaded_periods[/]")

                console.print(f"\n[success]Reset complete![/]")
                console.print(f"\nTo reload data: [bold]python scripts/pipeline.py scan --pipeline {pipeline}[/]")
