"""
Schema-based file grouping for multi-period bootstrap.

Groups files by column fingerprint so files with identical schemas
load to the same table, regardless of filename.
"""
from collections import defaultdict
from typing import Dict, List, Tuple
import pandas as pd


def get_fingerprint(path: str) -> tuple:
    """Extract column names as schema fingerprint."""
    try:
        if path.endswith('.csv'):
            cols = pd.read_csv(path, nrows=0).columns.tolist()
        else:
            cols = pd.read_excel(path, nrows=0).columns.tolist()
        # Normalize: lowercase, strip, ignore unnamed columns
        return tuple(
            c.lower().strip() for c in cols
            if not str(c).lower().startswith('unnamed')
        )
    except Exception:
        return ()


def group_by_schema(files_with_paths: List[Tuple]) -> Dict[tuple, List[Tuple]]:
    """
    Group files by schema fingerprint.

    Args:
        files_with_paths: List of (DiscoveredFile, local_path) tuples

    Returns:
        Dict mapping fingerprint -> list of (file, path) tuples
    """
    groups = defaultdict(list)
    for f, path in files_with_paths:
        fp = get_fingerprint(path)
        if fp:  # Skip files we can't read
            groups[fp].append((f, path))
    return dict(groups)


def pick_representative(group: List[Tuple]) -> Tuple:
    """Pick the file with latest period as representative for enrichment."""
    return max(group, key=lambda x: x[0].period or '')


def fingerprint_to_key(fp: tuple) -> str:
    """Convert fingerprint tuple to storable string key."""
    import hashlib
    return hashlib.md5(str(fp).encode()).hexdigest()[:12]


def extract_file_type(filename: str) -> str:
    """
    Extract logical file type from NHS filename.

    Examples:
        msds-oct2025-exp-data.csv → "data"
        msds-oct2025-exp-measures.csv → "measures"
        msds-oct2025-exp-dq.csv → "dq"
    """
    name = filename.lower()
    for suffix in ['data', 'measures', 'dq', 'quality', 'summary', 'detail']:
        if suffix in name:
            return suffix
    return "main"


def get_expected_columns(fp) -> set:
    """Get expected source columns from FilePattern's sheet mappings."""
    cols = set()
    for sm in fp.sheet_mappings:
        cols.update(sm.column_mappings.keys())
    return cols


def find_compatible_files(fp, period_files, temp_dir, already_matched=None):
    """
    Find files with compatible schema that don't match current patterns.

    Returns list of (file, local_path) tuples for compatible unmatched files.
    """
    import re
    from datawarp.loader import download_file

    already_matched = already_matched or []
    expected_cols = get_expected_columns(fp)
    if not expected_cols:
        return []

    compatible = []
    for f in period_files:
        # Skip if already matched or wrong file type
        if f in already_matched or f.file_type not in fp.file_types:
            continue
        # Skip if matches any existing pattern
        if any(re.match(p, f.filename, re.IGNORECASE) for p in fp.filename_patterns):
            continue

        # Download and check schema
        try:
            local_path = download_file(f.url, temp_dir)
            file_cols = set(get_fingerprint(local_path))

            # Check overlap (70% of expected columns exist)
            overlap = expected_cols & file_cols
            if len(overlap) / len(expected_cols) >= 0.7:
                compatible.append((f, local_path))
        except Exception:
            continue

    return compatible
