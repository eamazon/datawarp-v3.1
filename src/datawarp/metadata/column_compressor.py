"""Column Pattern Compression for LLM Enrichment.

Handles files with 100+ repetitive columns by detecting patterns,
compressing to samples for LLM, and expanding back to full column set.
"""
import re
from typing import List, Dict, Tuple, Optional
from collections import defaultdict


def detect_sequential_pattern(column_names: List[str]) -> Optional[Dict]:
    """Detect sequential numeric patterns in column names (10+ matching columns)."""
    if not column_names:
        return None
    groups = defaultdict(list)
    for col in column_names:
        match = re.match(r'^([a-z_]+?)_*(\d+)', col, re.IGNORECASE)
        if match:
            groups[match.group(1)].append(col)

    for prefix, cols in groups.items():
        if len(cols) >= 10 and _is_sequential(cols):
            pattern_str = f'{prefix}_{{n}}_{{n+1}}' if '_' in cols[0][len(prefix):] else f'{prefix}_{{n}}'
            return {'pattern': pattern_str, 'count': len(cols), 'columns': cols, 'prefix': prefix}
    return None


def _is_sequential(columns: List[str]) -> bool:
    """Check if columns follow a sequential numeric pattern."""
    numbers = []
    for col in sorted(columns):
        nums = re.findall(r'\d+', col)
        if nums:
            numbers.append(int(nums[0]))
    if len(numbers) < 2:
        return False
    sequential_count = sum(1 for i in range(len(numbers)-1) if numbers[i+1] - numbers[i] <= 2)
    return sequential_count / len(numbers) > 0.7


def compress_columns(columns: List[str]) -> Tuple[List[str], Optional[Dict]]:
    """Compress repetitive columns to samples + pattern info."""
    if not columns or len(columns) < 10:
        return columns if columns else [], None

    pattern = detect_sequential_pattern(columns)
    if not pattern:
        return columns, None

    pattern_cols = set(pattern['columns'])
    non_pattern_cols = [c for c in columns if c not in pattern_cols]
    sample_cols = pattern['columns'][:2] + pattern['columns'][-1:]

    pattern_info = {
        'pattern': pattern['pattern'], 'count': pattern['count'],
        'columns': pattern['columns'], 'prefix': pattern['prefix'], 'sample_columns': sample_cols
    }
    return non_pattern_cols + sample_cols, pattern_info


def expand_columns(compressed_result: Dict, pattern_info: Optional[Dict]) -> Dict:
    """Expand pattern-compressed columns back to full set."""
    if not pattern_info:
        return compressed_result
    columns_metadata = compressed_result.get('columns', {})
    if not columns_metadata:
        return compressed_result

    pattern_cols = pattern_info['columns']
    sample_col = pattern_cols[0]

    template_metadata = None
    for col_name, col_meta in columns_metadata.items():
        if col_name == sample_col or col_name.startswith(pattern_info['prefix']):
            template_metadata = col_meta
            break

    if not template_metadata:
        template_metadata = {
            'pg_name': None,
            'description': f"Sequential data point in {pattern_info['prefix']} series",
            'metadata': {'measure': True, 'dimension': False, 'tags': ['time_series', 'sequential']}
        }

    expanded_columns = columns_metadata.copy()
    for col in pattern_cols:
        if col not in expanded_columns:
            pg_name = template_metadata.get('pg_name', col)
            pg_name = pg_name.replace(sample_col, col) if pg_name else col
            expanded_columns[col] = {
                'pg_name': pg_name, 'description': template_metadata.get('description', ''),
                'metadata': template_metadata.get('metadata', {}).copy()
            }

    compressed_result['columns'] = expanded_columns
    return compressed_result


def compress_file_preview(file_entry: Dict) -> Tuple[Dict, Optional[Dict]]:
    """Compress repetitive columns in file preview for LLM."""
    preview = file_entry.get('preview', {})
    columns = preview.get('columns', [])

    if len(columns) < 50:
        return file_entry, None

    compressed_cols, pattern = compress_columns(columns)
    if not pattern:
        return file_entry, None

    compressed_entry = file_entry.copy()
    compressed_preview = preview.copy()
    compressed_preview['columns'] = compressed_cols
    compressed_preview['pattern_info'] = {
        'pattern': pattern['pattern'], 'count': pattern['count'],
        'sample_columns': pattern['sample_columns'], 'prefix': pattern['prefix']
    }

    if 'sample_rows' in compressed_preview:
        compressed_preview['sample_rows'] = [
            {col: row[col] for col in compressed_cols if col in row}
            for row in compressed_preview['sample_rows']
        ]

    compressed_entry['preview'] = compressed_preview
    return compressed_entry, pattern
