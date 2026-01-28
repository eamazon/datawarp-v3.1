"""Transform module for timeseries data handling.

Functions for unpivoting wide date formats and handling schema stability.
"""
from .unpivot import (
    parse_date_column,
    unpivot_wide_dates,
    detect_and_unpivot,
    is_date_column,
)

__all__ = [
    'parse_date_column',
    'unpivot_wide_dates',
    'detect_and_unpivot',
    'is_date_column',
]
