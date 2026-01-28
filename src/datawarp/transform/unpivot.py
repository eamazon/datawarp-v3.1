"""Universal Unpivot Engine - transforms wide date format to long format."""
import pandas as pd
import re
from typing import Optional, List, Tuple

MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}


def is_date_column(col_name: str) -> bool:
    """Check if column name looks like a date header."""
    if not col_name:
        return False
    return parse_date_column(col_name) is not None


def parse_date_column(col_name: str) -> Optional[str]:
    """Parse date column header to ISO date string (YYYY-MM-DD)."""
    if not col_name:
        return None
    col_lower = col_name.lower().strip()

    # Try "col_2009_09_30_00_00_00" pattern (sanitized datetime)
    match = re.match(r'col_(\d{4})_(\d{2})_(\d{2})', col_lower)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"

    # Try "Nov-25", "November 2025", "March_2020" patterns
    for month_str, month_num in MONTH_MAP.items():
        if col_lower.startswith(month_str):
            match = re.search(r'(\d{2,4})', col_lower)
            if match:
                year = int(match.group(1))
                if year < 100:
                    year += 2000
                return f"{year}-{month_num:02d}-01"

    # Try "2025-11" or "2025_11" pattern
    match = re.match(r'(\d{4})[-_](\d{1,2})', col_lower)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        if 1 <= month <= 12:
            return f"{year}-{month:02d}-01"

    # Try quarter pattern "Q1 2025", "Q2_2024"
    match = re.match(r'q([1-4])[-_\s]?(\d{2,4})', col_lower)
    if match:
        quarter, year = int(match.group(1)), int(match.group(2))
        if year < 100:
            year += 2000
        month = (quarter - 1) * 3 + 1
        return f"{year}-{month:02d}-01"

    return None


def unpivot_wide_dates(
    df: pd.DataFrame, static_columns: List[str], date_columns: List[str],
    value_name: str = 'value', period_name: str = 'period'
) -> pd.DataFrame:
    """Transform wide date format to long format."""
    missing_static = [c for c in static_columns if c not in df.columns]
    missing_date = [c for c in date_columns if c not in df.columns]
    if missing_static:
        raise ValueError(f"Static columns not found: {missing_static}")
    if missing_date:
        raise ValueError(f"Date columns not found: {missing_date}")

    df_long = pd.melt(df, id_vars=static_columns, value_vars=date_columns,
                      var_name='_raw_period', value_name=value_name)
    df_long[period_name] = df_long['_raw_period'].apply(parse_date_column)
    return df_long[static_columns + [period_name, '_raw_period', value_name]]


def detect_and_unpivot(df: pd.DataFrame, min_date_columns: int = 3) -> Tuple[pd.DataFrame, dict]:
    """Auto-detect wide date pattern and unpivot if found."""
    headers = df.columns.tolist()
    date_cols = [h for h in headers if is_date_column(h)]
    static_cols = [h for h in headers if h and not is_date_column(h)]

    metadata = {
        'transformed': False, 'date_columns': date_cols, 'static_columns': static_cols,
        'original_shape': df.shape, 'final_shape': df.shape
    }
    if len(date_cols) < min_date_columns:
        return df, metadata

    df_transformed = unpivot_wide_dates(df, static_cols, date_cols)
    metadata['transformed'] = True
    metadata['final_shape'] = df_transformed.shape
    return df_transformed, metadata
