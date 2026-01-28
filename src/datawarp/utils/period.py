"""Period parsing utilities - extract YYYY-MM from text"""
import re
from typing import Optional, List, Dict, Tuple
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

# Build regex for month names
MONTH_PATTERN = '|'.join(sorted(MONTHS.keys(), key=len, reverse=True))


def _extract_month_year(text: str) -> Optional[Tuple[str, str]]:
    """Smart extraction of month and year from text.

    Handles any order/format: "december 2025", "2025-12", "31-dec-2025", "122025", etc.
    Returns (year, month) tuple or None.
    """
    text_lower = text.lower()

    # Try to find month name
    month_match = re.search(rf'({MONTH_PATTERN})', text_lower)
    month_num = None

    if month_match:
        month_num = MONTHS.get(month_match.group(1))

    # Try to find 4-digit year
    year_match = re.search(r'(20[1-3]\d)', text_lower)
    year = year_match.group(1) if year_match else None

    # If we have both month name and year, we're done
    if month_num and year:
        return (year, month_num)

    # Try compact formats: YYYYMM or MMYYYY
    compact_match = re.search(r'(\d{6})', text_lower)
    if compact_match:
        digits = compact_match.group(1)
        # Try YYYYMM first
        if digits[:4].startswith('20') and 1 <= int(digits[4:6]) <= 12:
            return (digits[:4], digits[4:6])
        # Try MMYYYY
        if digits[2:6].startswith('20') and 1 <= int(digits[:2]) <= 12:
            return (digits[2:6], digits[:2])

    # Try 2-digit year with month name (nov25)
    if month_num:
        short_year = re.search(r'(\d{2})(?!\d)', text_lower)
        if short_year:
            yr = int(short_year.group(1))
            if 20 <= yr <= 35:  # 2020-2035
                return (f"20{yr}", month_num)

    # Try ISO format: 2024-11
    iso_match = re.search(r'(20[1-3]\d)[-_/](\d{2})', text_lower)
    if iso_match:
        yr, mo = iso_match.groups()
        if 1 <= int(mo) <= 12:
            return (yr, mo)

    return None


def parse_period(text: str) -> Optional[str]:
    """
    Extract YYYY-MM period from text.

    Handles flexibly:
    - 2024-11, 2024_11, 2024/11
    - november-2024, nov-2024, nov_2024
    - december 2025 (space separator)
    - 31-december-2025 (with day)
    - 202411 (YYYYMM compact)
    - 122025 (MMYYYY compact)
    - nov25 (abbreviated)

    Returns None if no period found.
    """
    if not text:
        return None

    result = _extract_month_year(text)
    if result:
        year, month = result
        return f"{year}-{month}"

    return None


def extract_period_from_url(url: str) -> Optional[str]:
    """
    Extract period from URL path, looking for month-year patterns in path segments.

    NHS URLs typically have periods in paths like:
    - /january-2025/
    - /december-2024/
    - /31-december-2025/
    - /2024-11/

    We look at path segments only, not query strings or hash codes.
    """
    if not url:
        return None

    from urllib.parse import urlparse
    parsed = urlparse(url)
    path = parsed.path

    # Split into segments and check each one for period patterns
    segments = [s for s in path.split('/') if s]

    for segment in segments:
        # Use the smart extraction on each segment
        result = _extract_month_year(segment)
        if result:
            year, month = result
            # Validate year is reasonable
            if 2010 <= int(year) <= 2035:
                return f"{year}-{month}"

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
        period = None

        # Try filename first
        filename = f.get('filename', '')
        if filename:
            period = parse_period(filename)

        # If no period in filename, try URL path segments (period in /january-2025/)
        if not period:
            url = f.get('url', '')
            if url:
                period = extract_period_from_url(url)

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
