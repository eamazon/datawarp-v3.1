"""Canonicalize source codes and column names by removing date/period patterns.

Ensures cross-period consolidation by stripping temporal identifiers
from LLM-generated source codes and column names, enabling consistent naming.
"""
import re
from typing import List, Set, Tuple, Optional

# Tokens that indicate date/time information (filtered during semantic comparison)
DATE_TOKENS: Set[str] = {
    'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
    'january', 'february', 'march', 'april', 'june', 'july', 'august',
    'september', 'october', 'november', 'december',
    'from', 'to', 'since', 'until', 'as', 'at', 'of', 'the', 'by',
    'q1', 'q2', 'q3', 'q4', 'quarter', 'fy', 'ytd', 'mtd', 'yoy', 'mom',
}


def remove_date_patterns(code: str) -> str:
    """Remove date/period patterns from source code.

    Examples: adhd_may25_data -> adhd_data, mhsds_historic_2025_05 -> mhsds_historic
    """
    if not code:
        return code

    canonical = code
    # Remove month-year combined patterns FIRST (may25, apr2024, etc.)
    canonical = re.sub(
        r'_(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\d{2,4}',
        '', canonical, flags=re.IGNORECASE
    )
    # Remove ISO date patterns (2025_05, 2025-05, etc.)
    canonical = re.sub(r'_?\d{4}[-_]\d{2}', '', canonical)
    # Remove year patterns (2023, 2024, 2025, etc.)
    canonical = re.sub(r'_?20\d{2}', '', canonical)
    # Remove full month names
    canonical = re.sub(
        r'_(january|february|march|april|may|june|july|august|september|october|november|december)',
        '', canonical, flags=re.IGNORECASE
    )
    # Remove month abbreviations
    canonical = re.sub(
        r'_(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
        '', canonical, flags=re.IGNORECASE
    )
    # Remove quarter patterns (q1, q2, q3, q4)
    canonical = re.sub(r'_?q[1-4]', '', canonical, flags=re.IGNORECASE)
    # Remove standalone 2-digit numbers that might be year shorts (25, 24, etc.)
    canonical = re.sub(r'_\d{2}(?=_|$)', '', canonical)
    # Remove standalone month numbers (01-12)
    canonical = re.sub(r'_0?[1-9](?=_|$)', '', canonical)
    canonical = re.sub(r'_1[0-2](?=_|$)', '', canonical)
    # Clean up multiple underscores and trailing/leading underscores
    canonical = re.sub(r'_+', '_', canonical)
    return canonical.strip('_')


def tokenize_column_name(name: str) -> Set[str]:
    """Extract semantic tokens from column name, filtering date-related noise."""
    if not name:
        return set()
    tokens = set(re.findall(r'[a-z]+', name.lower()))
    tokens -= DATE_TOKENS
    return {t for t in tokens if len(t) > 2 and not t.isdigit()}


def jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
    """Calculate Jaccard similarity between two token sets."""
    if not set1 and not set2:
        return 1.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def semantic_column_similarity(col1: str, col2: str) -> float:
    """Calculate semantic similarity between two column names (0.0-1.0).

    Compares columns by their semantic tokens, ignoring date/period variations.
    """
    tokens1 = tokenize_column_name(col1)
    tokens2 = tokenize_column_name(col2)
    return jaccard_similarity(tokens1, tokens2)


def match_column_to_reference(
    new_col: str, reference_cols: List[str], threshold: float = 0.8
) -> Tuple[Optional[str], float]:
    """Find best matching reference column using semantic similarity.

    Returns (best_match_name, similarity_score) or (None, score) if below threshold.
    """
    if not reference_cols:
        return (None, 0.0)

    best_match: Optional[str] = None
    best_similarity = 0.0

    for ref_col in reference_cols:
        similarity = semantic_column_similarity(new_col, ref_col)
        if similarity > best_similarity:
            best_similarity = similarity
            best_match = ref_col

    if best_similarity >= threshold:
        return (best_match, best_similarity)
    return (None, best_similarity)
