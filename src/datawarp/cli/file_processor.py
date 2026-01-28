"""
File processing utilities for DataWarp CLI.

Handles CSV, Excel (xlsx/xls), and ZIP file processing with grain detection
and optional LLM enrichment.
"""
import os
import re
from typing import List, Tuple
from urllib.parse import unquote

import pandas as pd

from datawarp.loader import (
    load_sheet, load_file, download_file, get_sheet_names,
    extract_zip, list_zip_contents, FileExtractor,
)
from datawarp.metadata import detect_grain, enrich_sheet
from datawarp.pipeline import SheetMapping, PipelineConfig, record_load, save_config
from datawarp.utils import sanitize_name, make_table_name


def _enrich_and_load(
    df: pd.DataFrame,
    sheet_name: str,
    local_path: str,
    auto_id: str,
    period: str,
    enrich: bool,
    console,
    is_csv: bool = False,
) -> Tuple[SheetMapping, int]:
    """Common enrichment and loading logic for both CSV and Excel files."""
    grain_info = detect_grain(df)
    grain, grain_col, grain_desc = grain_info['grain'], grain_info['grain_column'], grain_info['description']

    table_name = make_table_name(auto_id, sheet_name)
    table_desc = f"Data from {sheet_name}"
    sanitized_cols = [sanitize_name(str(c)) for c in df.columns]
    col_mappings = {c: c for c in sanitized_cols}
    col_descriptions = {}

    if enrich:
        if is_csv:
            console.print("  [yellow]Enriching with LLM...[/]")
        enriched = enrich_sheet(
            sheet_name=sheet_name, columns=sanitized_cols,
            sample_rows=df.head(3).to_dict('records'),
            publication_hint=auto_id, grain_hint=grain,
            pipeline_id=auto_id, source_file=local_path
        )
        table_name = f"tbl_{sanitize_name(enriched['table_name'])}"[:63]
        table_desc = enriched['table_description']
        col_mappings = enriched['columns']
        col_descriptions = enriched['descriptions']
        console.print(f"{'  ' if is_csv else '    '}[green]LLM suggested: {table_name}[/]")

    # Load data
    if is_csv:
        rows, learned_mappings, col_types = load_file(
            local_path, table_name, period=period, column_mappings=col_mappings
        )
    else:
        rows, learned_mappings, col_types = load_sheet(
            local_path, sheet_name, table_name, period=period, column_mappings=col_mappings
        )

    if rows > 0:
        indent = "  " if is_csv else "    "
        msg = f"Loaded {rows} rows" if is_csv else f"{sheet_name}: {rows} rows"
        console.print(f"{indent}{msg} ({grain})")
        return SheetMapping(
            sheet_pattern='' if is_csv else sheet_name,
            table_name=table_name, table_description=table_desc,
            column_mappings=learned_mappings, column_descriptions=col_descriptions,
            column_types=col_types, grain=grain,
            grain_column=grain_col, grain_description=grain_desc,
        ), rows
    return None, 0


def process_data_file(
    local_path: str, filename: str, file_type: str,
    period: str, auto_id: str, enrich: bool, console,
) -> List[Tuple[SheetMapping, int]]:
    """
    Process a single data file (CSV, Excel, or ZIP) and return sheet mappings.
    For ZIP files, extracts and recursively processes each data file inside.
    """
    results = []

    if file_type == 'zip':
        console.print("  [dim]Extracting ZIP contents...[/]")
        zip_contents = list_zip_contents(local_path)
        console.print(f"  Found {len(zip_contents)} data files in ZIP")

        for extracted_path in extract_zip(local_path):
            ext_filename = os.path.basename(extracted_path)
            ext_type = os.path.splitext(ext_filename)[1].lower().lstrip('.')
            console.print(f"\n  [bold white]-> {ext_filename}[/]")
            results.extend(process_data_file(
                extracted_path, ext_filename, ext_type, period, auto_id, enrich, console
            ))
        return results

    elif file_type in ['xlsx', 'xls']:
        sheets = get_sheet_names(local_path)
        console.print(f"  {len(sheets)} sheet(s)")

        for sheet in sheets:
            try:
                extractor = FileExtractor(local_path, sheet)
                df = extractor.to_dataframe()
                if df.empty:
                    continue
                result, rows = _enrich_and_load(
                    df, sheet, local_path, auto_id, period, enrich, console
                )
                if result:
                    results.append((result, rows))
            except Exception as e:
                console.print(f"    [dim]{sheet}: skipped ({e})[/]")
        return results

    else:  # CSV
        try:
            df = pd.read_csv(local_path, low_memory=False)
        except Exception as e:
            console.print(f"  [red]Error reading: {e}[/]")
            return results

        sheet_name = os.path.splitext(filename)[0]
        result, rows = _enrich_and_load(
            df, sheet_name, local_path, auto_id, period, enrich, console, is_csv=True
        )
        if result:
            results.append((result, rows))
        return results


def load_period_files(
    config: PipelineConfig, period: str, period_files: List, temp_dir: str, console,
) -> List[Tuple[str, int]]:
    """
    Load all files for a period using config patterns.
    Consolidates loading logic from scan and backfill commands.

    Includes drift detection: if new columns are found, they're added with
    identity mappings and the config is saved with bumped version.
    """
    results = []
    config_modified = False

    for fp in config.file_patterns:
        matching = [f for f in period_files if re.match(fp.filename_pattern, f.filename, re.IGNORECASE)]

        if not matching:
            console.print(f"  [yellow]No file matching pattern: {fp.filename_pattern}[/]")
            continue

        for f in matching:
            console.print(f"  Processing: {unquote(f.filename)}")

            with console.status("Downloading..."):
                local_path = download_file(f.url, temp_dir)

            for sm in fp.sheet_mappings:
                # Track version before loading (drift detection may bump it)
                version_before = sm.mappings_version

                with console.status(f"Loading {sm.sheet_pattern or 'data'}..."):
                    if f.file_type == 'csv' or not sm.sheet_pattern:
                        rows, _, _ = load_file(
                            local_path, sm.table_name, period=period,
                            column_mappings=sm.column_mappings,
                            sheet_mapping=sm,  # Pass for drift detection
                        )
                    else:
                        rows, _, _ = load_sheet(
                            local_path, sm.sheet_pattern, sm.table_name,
                            period=period, column_mappings=sm.column_mappings,
                            sheet_mapping=sm,  # Pass for drift detection
                        )

                # Check if drift was detected (version bumped)
                if sm.mappings_version > version_before:
                    config_modified = True

                if rows > 0:
                    console.print(f"    [green]{sm.table_name}: {rows} rows[/]")
                    record_load(config.pipeline_id, period, sm.table_name, f.filename, sm.sheet_pattern, rows)
                    results.append((sm.table_name, rows))
                else:
                    console.print(f"    [dim]{sm.table_name}: skipped (sheet not found)[/]")

    # Save config if drift was detected (new columns added)
    if config_modified:
        console.print("[yellow]Config updated with new column mappings[/]")
        save_config(config)

    return results
