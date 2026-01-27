"""Load Excel/CSV/ZIP files to PostgreSQL with the critical column fix"""
import os
import tempfile
import zipfile
from io import StringIO
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from ..storage import get_connection
from ..utils.sanitize import sanitize_name
from .extractor import FileExtractor, get_sheet_names, clear_workbook_cache


def download_file(url: str, target_dir: Optional[str] = None) -> str:
    """
    Download a file from URL to local path.

    Returns the local file path.
    """
    if target_dir is None:
        target_dir = tempfile.mkdtemp()

    filename = url.split('/')[-1].split('?')[0]
    local_path = os.path.join(target_dir, filename)

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with open(local_path, 'wb') as f:
        f.write(response.content)

    return local_path


def extract_zip(zip_path: str, target_dir: Optional[str] = None) -> List[str]:
    """
    Extract a zip file and return paths to data files inside.

    Returns list of paths to CSV/Excel files found in the zip.
    """
    if target_dir is None:
        target_dir = tempfile.mkdtemp()

    data_files = []
    data_extensions = {'.csv', '.xlsx', '.xls'}

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            # Skip directories and hidden files
            if name.endswith('/') or name.startswith('__') or name.startswith('.'):
                continue

            ext = os.path.splitext(name)[1].lower()
            if ext in data_extensions:
                # Extract file
                zf.extract(name, target_dir)
                data_files.append(os.path.join(target_dir, name))

    return data_files


def list_zip_contents(zip_path: str) -> List[dict]:
    """
    List data files inside a zip without extracting.

    Returns list of dicts with 'filename', 'size', 'file_type'.
    """
    contents = []
    data_extensions = {'.csv', '.xlsx', '.xls'}

    with zipfile.ZipFile(zip_path, 'r') as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            name = info.filename
            # Skip hidden/system files
            if name.startswith('__') or name.startswith('.'):
                continue

            ext = os.path.splitext(name)[1].lower()
            if ext in data_extensions:
                contents.append({
                    'filename': os.path.basename(name),
                    'path': name,
                    'size': info.file_size,
                    'file_type': ext.lstrip('.'),
                })

    return contents


def load_file(
    file_path: str,
    table_name: str,
    schema: str = 'staging',
    period: Optional[str] = None,
    sheet_name: Optional[str] = None,
    column_mappings: Optional[Dict[str, str]] = None,
) -> Tuple[int, Dict[str, str], Dict[str, str]]:
    """
    Load a file (Excel, CSV, or ZIP) to PostgreSQL.

    For ZIP files, extracts and loads the first data file found.
    Use extract_zip() for more control over which files to load.

    Returns:
        Tuple of (rows_loaded, final_column_mappings, column_types)
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == '.csv':
        df = pd.read_csv(file_path, low_memory=False)
        return load_dataframe(df, table_name, schema, period, column_mappings)
    elif ext in ['.xlsx', '.xls']:
        # Use FileExtractor for Excel files
        return load_sheet(file_path, sheet_name or 0, table_name, schema, period, column_mappings)
    elif ext == '.zip':
        # Extract and load first data file
        extracted = extract_zip(file_path)
        if not extracted:
            raise ValueError(f"No data files (CSV/Excel) found in zip: {file_path}")
        # Load the first file found
        return load_file(extracted[0], table_name, schema, period, sheet_name, column_mappings)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def load_sheet(
    file_path: str,
    sheet_name: str,
    table_name: str,
    schema: str = 'staging',
    period: Optional[str] = None,
    column_mappings: Optional[Dict[str, str]] = None,
) -> Tuple[int, Dict[str, str], Dict[str, str]]:
    """
    Load a specific Excel sheet to PostgreSQL using FileExtractor.

    CRITICAL: Uses FileExtractor for structure detection, then DataFrame as
    single source of truth for DDL and COPY (prevents column drift).

    Returns:
        Tuple of (rows_loaded, final_column_mappings, column_types)
        Returns (0, {}, {}) if sheet doesn't exist
    """
    # Handle sheet_name as int (index) or str (name)
    if isinstance(sheet_name, int):
        try:
            sheets = get_sheet_names(file_path)
            if sheet_name < len(sheets):
                sheet_name = sheets[sheet_name]
            else:
                return 0, {}, {}
        except Exception:
            return 0, {}, {}

    try:
        # Use FileExtractor for sophisticated header detection
        extractor = FileExtractor(file_path, sheet_name)
        df = extractor.to_dataframe()

        if df.empty:
            return 0, {}, {}

        # Get column types from extractor
        structure = extractor.infer_structure()
        extractor_types = {}
        for col_info in structure.columns.values():
            extractor_types[col_info.pg_name] = col_info.inferred_type

        return load_dataframe(df, table_name, schema, period, column_mappings, extractor_types)

    except ValueError as e:
        if "not found" in str(e):
            # Sheet doesn't exist in this file - skip it
            return 0, {}, {}
        raise


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema: str = 'staging',
    period: Optional[str] = None,
    column_mappings: Optional[Dict[str, str]] = None,
    extractor_types: Optional[Dict[str, str]] = None,
) -> Tuple[int, Dict[str, str], Dict[str, str]]:
    """
    Load a DataFrame to PostgreSQL.

    THE CRITICAL COLUMN FIX:
    1. Sanitize columns ONCE at the start
    2. Apply mappings to DataFrame
    3. Use df.columns for DDL AND COPY
    4. CANNOT DRIFT because same source

    Args:
        df: DataFrame to load
        table_name: Target table name
        schema: Database schema
        period: Period string (e.g., "2024-11")
        column_mappings: Optional column name mappings
        extractor_types: Optional type hints from FileExtractor

    Returns:
        Tuple of (rows_loaded, final_column_mappings, column_types)
    """
    column_mappings = column_mappings or {}
    extractor_types = extractor_types or {}

    # Skip empty DataFrames
    if df.empty:
        return 0, {}, {}

    # Remove completely empty rows and columns
    df = df.dropna(how='all').dropna(axis=1, how='all')

    if df.empty:
        return 0, {}, {}

    # Drop unnamed columns (often navigation links or empty headers)
    unnamed_cols = [c for c in df.columns if str(c).lower().startswith('unnamed')]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)

    # =========================================================
    # STEP 1: Sanitize and map columns ONCE
    # This is the SINGLE SOURCE OF TRUTH
    # =========================================================
    final_columns = {}
    for orig_col in df.columns:
        # Sanitize the original column name
        sanitized = sanitize_name(str(orig_col))
        # Apply mapping if exists, otherwise use sanitized
        canonical = column_mappings.get(sanitized, sanitized)
        final_columns[orig_col] = canonical

    # Apply to DataFrame - THIS IS NOW THE TRUTH
    df = df.rename(columns=final_columns)

    # Handle duplicate column names (add suffix)
    seen = {}
    new_cols = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    df.columns = new_cols

    # Add period column if provided
    if period:
        df['period'] = period

    # =========================================================
    # STEP 2: Infer PostgreSQL types from DataFrame
    # Use extractor_types if available (more sophisticated)
    # =========================================================
    column_types = {}
    col_defs = []

    for col in df.columns:
        # Use extractor type if available, otherwise infer from DataFrame
        if col in extractor_types:
            pg_type = extractor_types[col]
        else:
            pg_type = _infer_pg_type(df[col])
        column_types[col] = pg_type
        col_defs.append(f'"{col}" {pg_type}')

    # =========================================================
    # STEP 3: Create table DDL using df.columns
    # SAME SOURCE as the COPY statement
    # =========================================================
    full_table = f'{schema}.{table_name}'

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            {', '.join(col_defs)}
        )
    """

    # =========================================================
    # STEP 4: COPY data using df.columns
    # CANNOT DRIFT - same column list as DDL
    # =========================================================
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Create table (if new)
            cur.execute(ddl)

            # Handle schema evolution: add missing columns
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (schema, table_name))
            existing_cols = {row[0] for row in cur.fetchall()}

            for col in df.columns:
                if col not in existing_cols:
                    pg_type = column_types.get(col, 'TEXT')
                    cur.execute(f'ALTER TABLE {full_table} ADD COLUMN "{col}" {pg_type}')

            # Prepare data for COPY
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)

            # Build column list from df.columns (SAME as DDL)
            columns_quoted = ', '.join(f'"{c}"' for c in df.columns)

            # COPY data
            cur.copy_expert(
                f"COPY {full_table} ({columns_quoted}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
                buffer
            )

            rows_loaded = len(df)

    # Return the mappings we learned (sanitized -> canonical)
    learned_mappings = {sanitize_name(str(k)): v for k, v in final_columns.items()}

    return rows_loaded, learned_mappings, column_types


def _infer_pg_type(series: pd.Series) -> str:
    """
    Infer PostgreSQL type from pandas Series.

    Conservative approach - use TEXT for ambiguous cases.
    """
    # Drop nulls for type inference
    non_null = series.dropna()

    if non_null.empty:
        return 'TEXT'

    dtype = series.dtype

    # Numeric types
    if pd.api.types.is_integer_dtype(dtype):
        max_val = non_null.abs().max()
        if max_val < 32767:
            return 'SMALLINT'
        elif max_val < 2147483647:
            return 'INTEGER'
        else:
            return 'BIGINT'

    if pd.api.types.is_float_dtype(dtype):
        return 'NUMERIC'

    # Boolean
    if pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'

    # Datetime
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'

    # String types - check for patterns
    if pd.api.types.is_string_dtype(dtype) or dtype == object:
        sample = non_null.head(100).astype(str)

        # Check if all values are short (likely codes)
        max_len = sample.str.len().max()
        if max_len <= 20:
            return 'VARCHAR(50)'
        elif max_len <= 100:
            return 'VARCHAR(255)'
        else:
            return 'TEXT'

    return 'TEXT'


def preview_sheet(file_path: str, sheet_name: str, nrows: int = 5) -> pd.DataFrame:
    """Preview first N rows of a sheet using FileExtractor."""
    try:
        extractor = FileExtractor(file_path, sheet_name)
        df = extractor.to_dataframe()
        return df.head(nrows)
    except Exception:
        # Fallback to simple pandas read
        return pd.read_excel(file_path, sheet_name=sheet_name, nrows=nrows)


# Re-export from extractor
__all__ = [
    'download_file',
    'load_file',
    'load_sheet',
    'load_dataframe',
    'preview_sheet',
    'get_sheet_names',
    'clear_workbook_cache',
]
