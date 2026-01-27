"""Grain detection - detect entity type from data values"""
import re
from typing import Dict, Optional
import pandas as pd


ENTITY_PATTERNS = {
    'trust': {
        'pattern': r'^R[A-Z0-9]{1,4}$',
        'description': 'NHS Trust level',
        'examples': ['RJ1', 'RXH', 'R0A'],
        'priority': 100
    },
    'icb': {
        'pattern': r'^Q[A-Z0-9]{2}$',
        'description': 'Integrated Care Board level',
        'examples': ['QWE', 'QOP', 'QHG'],
        'priority': 100
    },
    'gp_practice': {
        'pattern': r'^[A-Z][0-9]{5}$',
        'description': 'GP Practice level',
        'examples': ['A81001', 'B82001'],
        'priority': 100
    },
    'region': {
        'pattern': r'^Y[0-9]{2}$',
        'description': 'NHS Region level',
        'examples': ['Y56', 'Y58', 'Y59'],
        'priority': 50
    },
    'national': {
        'pattern': None,
        'keywords': ['ENGLAND', 'NATIONAL', 'TOTAL', 'ALL'],
        'description': 'National aggregate',
        'priority': 10
    }
}

MEASURE_KEYWORDS = [
    'count', 'total', 'number', 'percent', 'rate', 'ratio',
    'average', 'mean', 'median', 'sum', 'referrals', 'waiting'
]


def detect_grain(df: pd.DataFrame) -> Dict:
    """
    Scan first few columns for entity codes.

    Returns:
        {
            "grain": "icb",  # or "trust", "gp_practice", "national", "unknown"
            "grain_column": "org_code",  # which column has the entity
            "confidence": 0.95,
            "description": "ICB level data"
        }
    """
    if df.empty:
        return {"grain": "unknown", "grain_column": None, "confidence": 0, "description": ""}

    # Check first 10 columns
    for col in list(df.columns)[:10]:
        if _is_measure_column(str(col)):
            continue

        values = df[col].dropna().head(50).astype(str).str.upper().str.strip().tolist()
        if not values:
            continue

        # Check each entity pattern
        for entity_type, config in ENTITY_PATTERNS.items():
            if config.get('pattern'):
                matches = sum(1 for v in values if re.match(config['pattern'], v))
                confidence = matches / len(values) if values else 0

                if confidence >= 0.7:
                    return {
                        "grain": entity_type,
                        "grain_column": str(col),
                        "confidence": round(confidence, 2),
                        "description": config['description']
                    }

            elif config.get('keywords'):
                all_text = ' '.join(values)
                if any(kw in all_text for kw in config['keywords']):
                    return {
                        "grain": "national",
                        "grain_column": None,
                        "confidence": 0.8,
                        "description": "National aggregate data"
                    }

    return {"grain": "unknown", "grain_column": None, "confidence": 0, "description": ""}


def _is_measure_column(col_name: str) -> bool:
    """Columns with these keywords are measures, not entities."""
    col_lower = col_name.lower()
    return any(m in col_lower for m in MEASURE_KEYWORDS)
