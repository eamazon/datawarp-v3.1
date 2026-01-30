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


def _deduplicate_files(file_tuples: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """Deduplicate CSV/XLSX pairs, keeping XLSX (richer format).

    Args:
        file_tuples: List of (extracted_path, relative_path_in_zip) tuples

    Groups files by base name (without extension and format suffix like _csv/_xlsx),
    returns only the preferred format from each group.
    """
    # Priority: xlsx > xls > csv (lower = better)
    PRIORITY = {'.xlsx': 1, '.xls': 2, '.csv': 3}

    groups = {}
    for extracted_path, relative_path in file_tuples:
        name = os.path.basename(relative_path)
        # Remove extension and format suffix: "file_Aug24_csv.csv" → "file_Aug24"
        base = re.sub(r'_(csv|xlsx|xls)?\.(csv|xlsx|xls)$', '', name, flags=re.I)
        # Also remove date range suffix for grouping: "file_Sep24-Aug25" → "file"
        base = re.sub(r'_[A-Za-z]{3}\d{2}-[A-Za-z]{3}\d{2}$', '', base)

        if base not in groups:
            groups[base] = []
        groups[base].append((extracted_path, relative_path))

    # Select preferred format from each group
    result = []
    for tuples in groups.values():
        if len(tuples) == 1:
            result.append(tuples[0])
        else:
            best = min(tuples, key=lambda t: PRIORITY.get(os.path.splitext(t[0])[1].lower(), 99))
            result.append(best)

    return result

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
    name_registry=None,
    source_context: str = None,
) -> Tuple[SheetMapping, int, int, int]:
    """Common enrichment and loading logic for both CSV and Excel files.

    Returns: (SheetMapping, rows_loaded, source_rows, source_columns)
    """
    # Track source metrics for reconciliation
    source_rows = len(df)
    source_columns = len(df.columns)

    grain_info = detect_grain(df)
    grain, grain_col, grain_desc = grain_info['grain'], grain_info['grain_column'], grain_info['description']

    suggested_name = make_table_name(auto_id, sheet_name)
    table_desc = f"Data from {sheet_name}"
    sanitized_cols = [sanitize_name(str(c)) for c in df.columns]
    col_mappings = {c: c for c in sanitized_cols}
    col_descriptions = {}

    if enrich:
        if is_csv:
            console.print("  [warning]Enriching with LLM...[/]")
        enriched = enrich_sheet(
            sheet_name=sheet_name, columns=sanitized_cols,
            sample_rows=df.head(3).to_dict('records'),
            publication_hint=auto_id, grain_hint=grain,
            pipeline_id=auto_id, source_file=local_path
        )
        suggested_name = f"tbl_{sanitize_name(enriched['table_name'])}"[:63]
        table_desc = enriched['table_description']
        col_mappings = enriched['columns']
        col_descriptions = enriched['descriptions']
        console.print(f"{'  ' if is_csv else '    '}[success]LLM suggested: {suggested_name}[/]")

    # Resolve collisions if registry provided
    table_name = name_registry.register(suggested_name, source_context or local_path) if name_registry else suggested_name
    if table_name != suggested_name:
        console.print(f"{'  ' if is_csv else '    '}[warning]Name collision resolved: → {table_name}[/]")

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
        ), rows, source_rows, source_columns
    return None, 0, source_rows, source_columns


def process_data_file(
    local_path: str, filename: str, file_type: str,
    period: str, auto_id: str, enrich: bool, console,
    name_registry=None, zip_context: str = None,
) -> List[Tuple[SheetMapping, int]]:
    """
    Process a single data file (CSV, Excel, or ZIP) and return sheet mappings.
    For ZIP files, extracts and recursively processes each data file inside.

    Args:
        zip_context: For files extracted from ZIP, the context string like
            "zipfile.zip/folder/subfolder" to build full source_path provenance.
    """
    results = []

    if file_type == 'zip':
        console.print("  [muted]Extracting ZIP contents...[/]")
        zip_contents = list_zip_contents(local_path)
        extracted_tuples = list(extract_zip(local_path))
        original_count = len(extracted_tuples)

        # Deduplicate CSV/XLSX pairs (prefer XLSX)
        extracted_tuples = _deduplicate_files(extracted_tuples)
        if len(extracted_tuples) < original_count:
            skipped = original_count - len(extracted_tuples)
            console.print(f"  Found {original_count} files, deduped to {len(extracted_tuples)} (skipped {skipped} duplicate formats)")
        else:
            console.print(f"  Found {len(extracted_tuples)} data files in ZIP")

        for extracted_path, relative_path in extracted_tuples:
            ext_type = os.path.splitext(relative_path)[1].lower().lstrip('.')
            console.print(f"\n  [bold white]-> {relative_path}[/]")
            # Build ZIP context: "zipfile.zip/folder/file.xlsx"
            new_zip_context = f"{filename}/{relative_path}"
            results.extend(process_data_file(
                extracted_path, os.path.basename(relative_path), ext_type, period, auto_id, enrich, console,
                name_registry=name_registry, zip_context=new_zip_context
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
                # Build source context with ZIP path if from archive
                source_ctx = f"{zip_context}/{sheet}" if zip_context else f"{filename}/{sheet}"
                result, rows, source_rows, source_columns = _enrich_and_load(
                    df, sheet, local_path, auto_id, period, enrich, console,
                    name_registry=name_registry, source_context=source_ctx
                )
                if result:
                    # Store source metrics with result for record_load
                    result._source_rows = source_rows
                    result._source_columns = source_columns
                    result._source_path = source_ctx
                    results.append((result, rows))
            except Exception as e:
                console.print(f"    [muted]{sheet}: skipped ({e})[/]")
        return results

    else:  # CSV
        try:
            df = pd.read_csv(local_path, low_memory=False)
        except pd.errors.ParserError:
            # Malformed rows (often footers) - silently skip
            try:
                df = pd.read_csv(local_path, low_memory=False, on_bad_lines='skip')
            except Exception as e2:
                console.print(f"  [error]Error reading CSV: {e2}[/]")
                return results
        except Exception as e:
            console.print(f"  [error]Error reading: {e}[/]")
            return results

        sheet_name = os.path.splitext(filename)[0]
        # Use ZIP context if from archive, otherwise just filename
        source_ctx = zip_context if zip_context else filename
        result, rows, source_rows, source_columns = _enrich_and_load(
            df, sheet_name, local_path, auto_id, period, enrich, console, is_csv=True,
            name_registry=name_registry, source_context=source_ctx
        )
        if result:
            # Store source metrics with result for record_load
            result._source_rows = source_rows
            result._source_columns = source_columns
            result._source_path = source_ctx
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
        # Match if ANY pattern matches
        matching = [f for f in period_files
                    if any(re.match(p, f.filename, re.IGNORECASE) for p in fp.filename_patterns)]

        if not matching:
            # Try to find files with compatible schema
            from datawarp.cli.schema_grouper import find_compatible_files
            from datawarp.cli.helpers import make_filename_pattern
            from rich.prompt import Confirm

            compatible = find_compatible_files(fp, period_files, temp_dir)
            if compatible:
                sample_file = compatible[0][0]
                new_pattern = make_filename_pattern(sample_file.filename)
                console.print(f"  [warning]No match, but found {len(compatible)} file(s) with compatible schema:[/]")
                console.print(f"    {sample_file.filename}")
                if Confirm.ask(f"  Add pattern?", default=True):
                    fp.filename_patterns.append(new_pattern)
                    config_modified = True
                    # Re-match with updated patterns (only match files fitting the new pattern)
                    matching = [f for f in period_files
                                if any(re.match(p, f.filename, re.IGNORECASE) for p in fp.filename_patterns)]
                else:
                    continue
            else:
                console.print(f"  [warning]No file matching patterns[/]")
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
                    console.print(f"    [success]{sm.table_name}: {rows} rows[/]")
                    record_load(config.pipeline_id, period, sm.table_name, f.filename, sm.sheet_pattern, rows)
                    results.append((sm.table_name, rows))
                else:
                    console.print(f"    [muted]{sm.table_name}: skipped (sheet not found)[/]")

    # Save config if drift was detected (new columns added)
    if config_modified:
        console.print("[warning]Config updated with new column mappings[/]")
        save_config(config)

    return results
