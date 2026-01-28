"""
Enrich command - targeted re-enrichment of columns with empty descriptions.
"""
from datetime import datetime
from typing import Optional

import click
import pandas as pd

from datawarp.cli.console import console
from datawarp.metadata import enrich_sheet
from datawarp.pipeline import load_config, save_config
from datawarp.storage import get_connection
from datawarp.utils import sanitize_name


@click.command('enrich')
@click.option('--pipeline', required=True, help='Pipeline ID to enrich')
@click.option('--table', default=None, help='Specific table to enrich (default: all)')
@click.option('--force', is_flag=True, help='Re-enrich all columns, not just empty ones')
@click.option('--dry-run', is_flag=True, help='Show what would be enriched without calling LLM')
def enrich_command(pipeline: str, table: Optional[str], force: bool, dry_run: bool):
    """
    Re-enrich columns that have empty descriptions.

    Uses sample data from existing staging tables to generate semantic
    column names and descriptions via LLM. Only fills in empty descriptions
    unless --force is used.

    Examples:
        # Enrich all tables with empty descriptions
        python scripts/pipeline.py enrich --pipeline mi_adhd

        # Enrich a specific table
        python scripts/pipeline.py enrich --pipeline mi_adhd --table tbl_adhd_counts

        # Preview what would be enriched
        python scripts/pipeline.py enrich --pipeline mi_adhd --dry-run
    """
    config = load_config(pipeline)
    if not config:
        console.print(f"[red]Pipeline '{pipeline}' not found[/]")
        return

    console.print(f"\n[bold blue]Enriching:[/] {config.name}")
    console.print(f"[dim]Pipeline: {pipeline}[/]\n")

    tables_enriched = 0
    columns_enriched = 0

    for fp in config.file_patterns:
        for sm in fp.sheet_mappings:
            # Skip if specific table requested and this isn't it
            if table and sm.table_name != table:
                continue

            # Find columns needing enrichment
            cols_to_enrich = []
            for col, desc in sm.column_descriptions.items():
                if force or not desc:
                    cols_to_enrich.append(col)

            if not cols_to_enrich:
                console.print(f"[dim]{sm.table_name}: all columns have descriptions[/]")
                continue

            console.print(f"\n[bold cyan]{sm.table_name}[/]")
            console.print(f"  Columns needing enrichment: {len(cols_to_enrich)}")
            for col in cols_to_enrich[:5]:
                console.print(f"    - {col}")
            if len(cols_to_enrich) > 5:
                console.print(f"    ... and {len(cols_to_enrich) - 5} more")

            if dry_run:
                console.print("  [dim]Dry run - skipping LLM call[/]")
                continue

            # Get sample data from staging table
            try:
                sample_df = _get_sample_data(sm.table_name, limit=50)
                if sample_df.empty:
                    console.print(f"  [yellow]No data in table, skipping[/]")
                    continue
            except Exception as e:
                console.print(f"  [red]Error reading table: {e}[/]")
                continue

            # Get current column names (semantic names in the table)
            current_cols = [c for c in sample_df.columns if not c.startswith('_')]

            console.print("  [yellow]Calling LLM for enrichment...[/]")

            # Call enrichment
            enriched = enrich_sheet(
                sheet_name=sm.sheet_pattern or sm.table_name,
                columns=current_cols,
                sample_rows=sample_df.head(5).to_dict('records'),
                publication_hint=config.name,
                grain_hint=sm.grain,
                pipeline_id=pipeline,
                source_file=f"re-enrich:{sm.table_name}"
            )

            # Merge descriptions (only fill empty ones unless force)
            # Build reverse lookup: LLM may suggest new semantic names
            # enriched['columns'] = {original: new_semantic}
            # enriched['descriptions'] = {new_semantic: description}
            new_count = 0
            for col in cols_to_enrich:
                # Try multiple lookup strategies:
                # 1. Check if LLM suggested a new semantic name for this column
                new_semantic = enriched['columns'].get(col)
                new_desc = None

                if new_semantic:
                    new_desc = enriched['descriptions'].get(new_semantic, '')

                # 2. Fall back to looking up by current semantic name
                if not new_desc:
                    current_semantic = sm.column_mappings.get(col, col)
                    new_desc = enriched['descriptions'].get(current_semantic, '')

                # 3. Fall back to looking up by original column name
                if not new_desc:
                    new_desc = enriched['descriptions'].get(col, '')

                if new_desc:
                    if force or not sm.column_descriptions.get(col):
                        sm.column_descriptions[col] = new_desc
                        # Optionally update the semantic name mapping too
                        if new_semantic and new_semantic != col:
                            sm.column_mappings[col] = new_semantic
                        new_count += 1
                        console.print(f"    [green]✓[/] {col}: {new_desc[:50]}...")

            if new_count > 0:
                sm.last_enriched = datetime.now().isoformat()
                sm.mappings_version += 1
                tables_enriched += 1
                columns_enriched += new_count
                console.print(f"  [green]Added {new_count} descriptions, version → {sm.mappings_version}[/]")
            else:
                console.print(f"  [dim]No new descriptions from LLM[/]")

    # Save config if anything changed
    if tables_enriched > 0 and not dry_run:
        save_config(config)
        console.print(f"\n[green]✓ Enriched {columns_enriched} columns across {tables_enriched} table(s)[/]")
        console.print(f"[dim]Config saved[/]")
    elif dry_run:
        console.print(f"\n[dim]Dry run complete - no changes made[/]")
    else:
        console.print(f"\n[dim]No columns needed enrichment[/]")


def _get_sample_data(table_name: str, schema: str = 'staging', limit: int = 50) -> pd.DataFrame:
    """Get sample data from a staging table."""
    with get_connection() as conn:
        query = f'SELECT * FROM {schema}."{table_name}" LIMIT {limit}'
        return pd.read_sql(query, conn)
