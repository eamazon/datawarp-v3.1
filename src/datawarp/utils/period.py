"""Period parsing utilities - extract YYYY-MM from text"""
import re
from typing import Optional, List, Dict
from collections import defaultdict

# Month name mappings
MONTHS = {
    'january': '01', 'jan': '01',
    'february': '02', 'feb': '02',
    'march': '03', 'mar': '03',
    'april': '04', 'apr': '04',
    'may': '05',
    'june': '06', 'jun': '06',
    'july': '07', 'jul': '07',
    'august': '08', 'aug': '08',
    'september': '09', 'sep': '09', 'sept': '09',
    'october': '10', 'oct': '10',
    'november': '11', 'nov': '11',
    'december': '12', 'dec': '12',
}

# Period patterns in priority order
PERIOD_PATTERNS = [
    # ISO format: 2024-11, 2024_11
    (r'(\d{4})[-_](\d{2})(?!\d)', lambda m: f"{m.group(1)}-{m.group(2)}"),
    # Month-year: november-2024, nov-2024, november_2024
    (r'([a-z]+)[-_](\d{4})', lambda m: f"{m.group(2)}-{MONTHS.get(m.group(1).lower(), '00')}" if MONTHS.get(m.group(1).lower()) else None),
    # Year-month name: 2024-november, 2024_nov
    (r'(\d{4})[-_]([a-z]+)', lambda m: f"{m.group(1)}-{MONTHS.get(m.group(2).lower(), '00')}" if MONTHS.get(m.group(2).lower()) else None),
    # Compact: 202411
    (r'(\d{4})(\d{2})(?!\d)', lambda m: f"{m.group(1)}-{m.group(2)}" if 1 <= int(m.group(2)) <= 12 else None),
    # Abbreviated: nov25, aug25, may25 (month + 2-digit year)
    (r'([a-z]{3,9})(\d{2})(?!\d)', lambda m: f"20{m.group(2)}-{MONTHS.get(m.group(1).lower(), '00')}" if MONTHS.get(m.group(1).lower()) else None),
]


def parse_period(text: str) -> Optional[str]:
    """
    Extract YYYY-MM period from text.

    Handles:
    - 2024-11, 2024_11
    - november-2024, nov-2024
    - 2024-november
    - 202411

    Returns None if no period found.
    """
    if not text:
        return None

    text_lower = text.lower()

    for pattern, extractor in PERIOD_PATTERNS:
        match = re.search(pattern, text_lower, re.IGNORECASE)
        if match:
            result = extractor(match)
            if result and result != "00":
                # Validate month is 01-12
                month = result.split('-')[1]
                if 1 <= int(month) <= 12:
                    return result

    return None


def extract_periods_from_files(files: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group files by their detected period.

    Args:
        files: List of dicts with at least 'filename' or 'url' key

    Returns:
        Dict mapping period (YYYY-MM) to list of files
    """
    by_period = defaultdict(list)

    for f in files:
        # Try filename first, then URL
        text = f.get('filename', '') or f.get('url', '')
        period = parse_period(text)

        if period:
            by_period[period].append(f)
        else:
            by_period['unknown'].append(f)

    return dict(by_period)


def get_latest_period(periods: List[str]) -> Optional[str]:
    """Get the most recent period from a list."""
    valid = [p for p in periods if p != 'unknown']
    if not valid:
        return None
    return max(valid)


def sort_periods(periods: List[str], descending: bool = True) -> List[str]:
    """Sort periods chronologically."""
    valid = [p for p in periods if p != 'unknown']
    return sorted(valid, reverse=descending)
