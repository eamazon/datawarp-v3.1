"""
Shared utility functions for DataWarp CLI commands.
"""
import re
from typing import List


def group_files_by_period(files: List) -> dict:
    """Group DiscoveredFile objects by their period attribute."""
    by_period = {}
    for f in files:
        period = f.period or 'unknown'
        if period not in by_period:
            by_period[period] = []
        by_period[period].append({'filename': f.filename, 'url': f.url, 'file': f})
    return by_period


def infer_sheet_description(sheet_name: str) -> str:
    """Infer a description from sheet name."""
    name_lower = sheet_name.lower()

    if 'title' in name_lower or 'cover' in name_lower:
        return 'Title/cover page'
    if 'content' in name_lower:
        return 'Table of contents'
    if 'note' in name_lower or 'quality' in name_lower:
        return 'Notes/methodology'
    if 'definition' in name_lower:
        return 'Definitions'

    # Clean up table names
    clean = sheet_name.replace('_', ' ').replace('-', ' ')
    return f"Data: {clean}"


def extract_name_from_url(url: str) -> str:
    """Extract a reasonable name from URL path."""
    # e.g., /statistical/mi-adhd -> MI ADHD
    path = url.rstrip('/').split('/')[-1]
    name = path.replace('-', ' ').replace('_', ' ').title()
    return name


def make_filename_pattern(filename: str) -> str:
    """
    Create a regex pattern from a filename that will match similar files.

    e.g., "ADHD-Data-2024-11.xlsx" -> r"ADHD-Data-\\d{4}-\\d{2}\\.xlsx"
    e.g., "adhd_summary_nov25.xlsx" -> r"adhd_summary_[a-z]{3}\\d{2}\\.xlsx"
    """
    # Escape special regex chars
    pattern = re.escape(filename)

    # Replace date patterns with regex
    # YYYY-MM, YYYY_MM
    pattern = re.sub(r'2\d{3}[-_]\d{2}', r'\\d{4}[-_]\\d{2}', pattern)

    # Abbreviated month + 2-digit year: nov25, aug25, may25
    short_months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                    'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for month in short_months:
        # Match month followed by 2 digits (e.g., nov25)
        pattern = re.sub(f'{month}\\d{{2}}', r'[a-z]{3}\\d{2}', pattern, flags=re.IGNORECASE)

    # Full month names
    months = ['january', 'february', 'march', 'april', 'may', 'june',
              'july', 'august', 'september', 'october', 'november', 'december']
    for month in months:
        pattern = re.sub(month, r'[a-z]+', pattern, flags=re.IGNORECASE)

    return pattern
