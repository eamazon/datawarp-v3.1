"""Grain detection - detect entity type from data values.

Uses two-pass detection for hierarchical NHS tables:
1. Primary org columns with low threshold (0.3) for mixed entity columns
2. Standard detection with normal threshold (0.5) for all entity columns
"""
import re
from typing import Dict, List, Optional
import pandas as pd

# Thresholds
MIN_CONFIDENCE = 0.5  # Standard detection threshold
MIN_CONFIDENCE_ORG_COLUMN = 0.3  # Lower threshold for primary org columns
MIN_MATCHES = 3  # Minimum matching values to avoid false positives

# Primary org column patterns (hierarchical table detection)
PRIMARY_ORG_COLUMN_PATTERNS = [
    'org code', 'org_code', 'organisation code', 'organization code',
    'provider code', 'provider_code', 'org id', 'org_id', 'provider id', 'provider_id'
]

# Values to exclude from entity detection
EXCLUDE_VALUES = {
    'UNKNOWN', 'OTHER', 'UNSPECIFIED', 'N/A', 'NA', '-', '', 'NULL', 'NONE',
    'ALL PROVIDERS', 'ALL TRUSTS', 'ALL ICBS', 'SUPPRESSED', 'REDACTED', '*'
}

ENTITY_PATTERNS = {
    'trust': {
        'pattern': r'^R[A-Z0-9]{1,4}$', 'description': 'NHS Trust level',
        'examples': ['RJ1', 'RXH', 'R0A'], 'priority': 100
    },
    'icb': {
        'pattern': r'^Q[A-Z0-9]{2}$', 'description': 'Integrated Care Board level',
        'examples': ['QWE', 'QOP', 'QHG'], 'priority': 100
    },
    'gp_practice': {
        'pattern': r'^[A-Z][0-9]{5}$', 'description': 'GP Practice level',
        'examples': ['A81001', 'B82001'], 'priority': 100
    },
    'ccg': {
        'pattern': r'^[0-9]{2}[A-Z]$', 'description': 'Clinical Commissioning Group (legacy)',
        'examples': ['00J', '00K', '01A'], 'priority': 70
    },
    'region': {
        'pattern': r'^Y[0-9]{2}$', 'description': 'NHS Region level',
        'examples': ['Y56', 'Y58', 'Y59'], 'priority': 50
    },
    'national': {
        'pattern': None, 'keywords': ['ENGLAND', 'NATIONAL', 'TOTAL', 'ALL'],
        'description': 'National aggregate', 'priority': 10
    }
}

# Name-based patterns for when codes aren't present
NAME_PATTERNS = {
    'trust': {
        'keywords': ['NHS TRUST', 'NHS FOUNDATION TRUST', 'UNIVERSITY HOSPITAL'],
        'description': 'NHS Trust level (by name)', 'priority': 80
    },
    'icb': {
        'keywords': ['INTEGRATED CARE BOARD', ' ICB'],
        'description': 'Integrated Care Board (by name)', 'priority': 80
    },
}

# Extended measure keywords (filter out measure columns from entity detection)
MEASURE_KEYWORDS = [
    'count', 'total', 'number', 'percent', 'rate', 'ratio', 'average', 'mean',
    'median', 'sum', 'referrals', 'waiting', 'deliveries', 'admissions', 'episodes',
    'attendances', 'appointments', 'anaesthetic', 'caesarean', 'spontaneous',
    'surgical', 'stay', 'length', 'duration', 'days', 'hours', 'weeks', 'breaches',
    'waits', 'patients'
]

# Entity column keywords (for _is_likely_entity_column)
ENTITY_KEYWORDS = [
    'code', ' id', 'org', 'provider code', 'trust code', 'icb code', 'region code',
    'practice code', 'commissioner code', 'org name', 'provider name', 'trust name',
    'geography'
]


def _is_measure_column(col_name: str) -> bool:
    """Columns with these keywords are measures, not entities."""
    col_lower = col_name.lower()
    return any(m in col_lower for m in MEASURE_KEYWORDS)


def _is_primary_org_column(col_name: str) -> bool:
    """Check if column name indicates a primary organization column."""
    col_lower = col_name.lower().replace('_', ' ').replace('\n', ' ')
    return any(pattern in col_lower for pattern in PRIMARY_ORG_COLUMN_PATTERNS)


def _is_likely_entity_column(col_name: str) -> bool:
    """Check if column name suggests it contains entity codes/names."""
    col_lower = col_name.lower().replace('_', ' ').replace('\n', ' ')
    if any(keyword in col_lower for keyword in MEASURE_KEYWORDS):
        return False
    if len(col_name) <= 20:
        return True
    return any(keyword in col_lower for keyword in ENTITY_KEYWORDS)


def _clean_values(values: List[str]) -> List[str]:
    """Remove excluded values (UNKNOWN, N/A, etc.) from list."""
    return [v for v in values if v.upper() not in EXCLUDE_VALUES]


def _detect_entity_in_column(values: List[str], min_confidence: float = MIN_CONFIDENCE) -> Optional[Dict]:
    """Detect entity type from column values. Returns match dict or None."""
    if not values:
        return None
    best_match, best_priority = None, 0
    for entity_type, config in ENTITY_PATTERNS.items():
        if not config.get('pattern'):
            continue
        matches = sum(1 for v in values if re.match(config['pattern'], v))
        confidence = matches / len(values)
        if confidence >= min_confidence and matches >= MIN_MATCHES:
            priority = config['priority'] * confidence
            if priority > best_priority:
                best_priority = priority
                best_match = {
                    "grain": entity_type, "confidence": round(confidence, 2),
                    "description": config['description'], "priority": config['priority']
                }
    return best_match


def detect_grain(df: pd.DataFrame) -> Dict:
    """
    Scan DataFrame columns for entity codes to determine data granularity.

    Uses two-pass detection:
    1. Primary org columns with low threshold (0.3) for hierarchical tables
    2. Standard detection with normal threshold (0.5) for all entity columns

    Returns:
        {"grain": "icb", "grain_column": "org_code", "confidence": 0.95, "description": "..."}
    """
    if df.empty:
        return {"grain": "unknown", "grain_column": None, "confidence": 0, "description": ""}

    # ========== PASS 1: Primary Org Column Detection ==========
    primary_org_match = None
    for col in list(df.columns)[:10]:
        col_str = str(col)
        if _is_measure_column(col_str):
            continue
        if _is_primary_org_column(col_str):
            values = df[col].dropna().head(50).astype(str).str.upper().str.strip().tolist()
            values = _clean_values(values)
            if not values:
                continue
            match = _detect_entity_in_column(values, MIN_CONFIDENCE_ORG_COLUMN)
            if match:
                primary_org_match = match.copy()
                primary_org_match["grain_column"] = col_str

    # ========== PASS 2: Standard Entity Detection ==========
    best_match, best_priority = None, 0
    for col in list(df.columns)[:10]:
        col_str = str(col)
        if _is_measure_column(col_str):
            continue
        if not _is_likely_entity_column(col_str):
            continue
        values = df[col].dropna().head(50).astype(str).str.upper().str.strip().tolist()
        values = _clean_values(values)
        if not values:
            continue
        match = _detect_entity_in_column(values, MIN_CONFIDENCE)
        if match:
            priority = match["priority"] * match["confidence"]
            if priority > best_priority:
                best_priority = priority
                best_match = match.copy()
                best_match["grain_column"] = col_str

    # ========== DECISION: Resolve Hierarchical Tables ==========
    # Primary org columns (org_code, provider_code, etc.) represent the actual data grain.
    # Other entity columns (ics_code, nhse_code) are hierarchy columns for grouping.
    # Prefer primary org match when: (1) it has higher priority, OR (2) best_match is
    # from a hierarchy column (region/icb with different column name pattern).
    if primary_org_match and best_match:
        primary_priority = ENTITY_PATTERNS.get(primary_org_match["grain"], {}).get("priority", 0)
        best_priority_config = ENTITY_PATTERNS.get(best_match["grain"], {}).get("priority", 0)
        # Prefer primary org if it has high priority (trust, icb, gp_practice)
        if primary_priority >= 70:
            result = primary_org_match.copy()
            result.pop("priority", None)
            return result
    if best_match:
        result = best_match.copy()
        result.pop("priority", None)
        return result
    if primary_org_match:
        result = primary_org_match.copy()
        result.pop("priority", None)
        return result

    # ========== PASS 3: Name-based Detection (fallback) ==========
    for col in list(df.columns)[:10]:
        col_str = str(col)
        if _is_measure_column(col_str):
            continue
        values = df[col].dropna().head(50).astype(str).str.upper().str.strip().tolist()
        if not values:
            continue
        all_text = ' '.join(values)
        for entity_type, config in NAME_PATTERNS.items():
            keywords = config.get('keywords', [])
            if sum(1 for kw in keywords if kw in all_text) > 0:
                row_matches = sum(1 for v in values if any(kw in v for kw in keywords))
                confidence = row_matches / len(values) if values else 0
                if confidence >= 0.3 and row_matches >= MIN_MATCHES:
                    return {"grain": entity_type, "grain_column": col_str,
                            "confidence": round(confidence, 2), "description": config['description']}

    # ========== PASS 4: National Keywords (last resort) ==========
    for col in list(df.columns)[:10]:
        values = df[col].dropna().head(50).astype(str).str.upper().str.strip().tolist()
        if not values:
            continue
        all_text = ' '.join(values)
        national_config = ENTITY_PATTERNS.get('national', {})
        keywords = national_config.get('keywords', [])
        if any(kw in all_text for kw in keywords):
            return {"grain": "national", "grain_column": None,
                    "confidence": 0.8, "description": "National aggregate data"}

    return {"grain": "unknown", "grain_column": None, "confidence": 0, "description": ""}
