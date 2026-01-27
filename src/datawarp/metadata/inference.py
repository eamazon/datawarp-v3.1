"""Heuristic metadata inference - no LLM needed"""
import re
from typing import Dict, List, Optional, Any

from ..storage import get_connection


# NHS entity patterns - detect from sample values
NHS_ENTITIES = {
    'icb': {
        'pattern': r'^Q[A-Z]{2}$',
        'description': 'Integrated Care Board code',
        'example': 'QWE',
    },
    'trust': {
        'pattern': r'^R[A-Z0-9]{2,4}$',
        'description': 'NHS Trust code',
        'example': 'RJ1',
    },
    'gp_practice': {
        'pattern': r'^[A-Z]\d{5}$',
        'description': 'GP Practice code',
        'example': 'A81001',
    },
    'ccg': {
        'pattern': r'^[0-9]{2}[A-Z]$',
        'description': 'Clinical Commissioning Group code (legacy)',
        'example': '00Q',
    },
    'region': {
        'pattern': r'^Y[0-9]{2}$',
        'description': 'NHS Region code',
        'example': 'Y56',
    },
    'ods': {
        'pattern': r'^[A-Z0-9]{3,10}$',
        'description': 'Organisation Data Service code',
        'example': 'RJ122',
    },
}

# Column name patterns -> descriptions
COLUMN_PATTERNS = [
    (r'.*_count$', 'Count of {subject}'),
    (r'.*_rate$', 'Rate per population'),
    (r'.*_percentage$|.*_pct$|.*_percent$', 'Percentage value'),
    (r'.*referral.*', 'Referral-related metric'),
    (r'.*waiting.*', 'Waiting time or count'),
    (r'.*admission.*', 'Hospital admission metric'),
    (r'.*discharge.*', 'Hospital discharge metric'),
    (r'.*attendance.*', 'Attendance count'),
    (r'.*appointment.*', 'Appointment metric'),
    (r'.*patient.*', 'Patient-related metric'),
    (r'.*provider.*', 'Healthcare provider identifier'),
    (r'.*commissioner.*', 'Commissioning organisation'),
    (r'.*code$', 'Identifier code'),
    (r'.*name$', 'Name field'),
    (r'.*date$', 'Date field'),
    (r'.*period$', 'Time period'),
]

# Known columns with exact descriptions
KNOWN_COLUMNS = {
    'icb_code': 'Integrated Care Board identifier (QXX format)',
    'icb_name': 'Integrated Care Board name',
    'trust_code': 'NHS Trust identifier (RXX format)',
    'trust_name': 'NHS Trust name',
    'period': 'Reporting period in YYYY-MM format',
    'reporting_period': 'Reporting period in YYYY-MM format',
    'provider_code': 'Healthcare provider organisation code',
    'provider_name': 'Healthcare provider organisation name',
    'commissioner_code': 'Commissioning organisation code',
    'commissioner_name': 'Commissioning organisation name',
    'region_code': 'NHS Region code',
    'region_name': 'NHS Region name',
    'total_referrals': 'Total number of referrals received',
    'open_referrals': 'Number of currently open referrals',
    'closed_referrals': 'Number of closed referrals',
    'waiting_list': 'Number of patients on waiting list',
    'appointments': 'Number of appointments',
    'attendances': 'Number of attendances',
}


def infer_column_description(
    column_name: str,
    sample_values: Optional[List[Any]] = None,
    table_context: Optional[str] = None,
) -> str:
    """
    Infer a description for a column using heuristics.

    Args:
        column_name: The sanitized column name
        sample_values: Optional sample values from the column
        table_context: Optional context about the table (e.g., "ADHD referrals")

    Returns:
        Human-readable description
    """
    col_lower = column_name.lower()

    # Check known columns first
    if col_lower in KNOWN_COLUMNS:
        return KNOWN_COLUMNS[col_lower]

    # Check if values match an NHS entity pattern
    if sample_values:
        entity_type = infer_entity_type(sample_values)
        if entity_type:
            entity_info = NHS_ENTITIES[entity_type]
            return f"{entity_info['description']} (e.g., {entity_info['example']})"

    # Check column name patterns
    for pattern, template in COLUMN_PATTERNS:
        if re.match(pattern, col_lower):
            # Extract subject from column name if template needs it
            if '{subject}' in template:
                # Extract subject: referral_count -> referrals
                subject = col_lower.replace('_count', '').replace('_rate', '').replace('_', ' ')
                return template.format(subject=subject)
            return template

    # Default: humanize the column name
    humanized = column_name.replace('_', ' ').title()
    return f"{humanized} value"


def infer_entity_type(sample_values: List[Any]) -> Optional[str]:
    """
    Detect NHS entity type from sample values.

    Returns entity type key or None.
    """
    if not sample_values:
        return None

    # Convert to strings and filter nulls
    str_values = [str(v).strip() for v in sample_values if v is not None and str(v).strip()]

    if not str_values:
        return None

    # Check each entity pattern
    for entity_type, info in NHS_ENTITIES.items():
        pattern = info['pattern']
        matches = sum(1 for v in str_values if re.match(pattern, v))
        # If >50% match, likely this entity type
        if matches > len(str_values) * 0.5:
            return entity_type

    return None


def get_table_metadata(table_name: str, schema: str = 'staging') -> Dict:
    """
    Get metadata for a table including column descriptions.

    Returns dict with:
        - table_name
        - schema
        - row_count
        - columns: list of {name, type, description, sample_values}
    """
    full_table = f'{schema}.{table_name}'

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get row count
            cur.execute(f"SELECT COUNT(*) FROM {full_table}")
            row_count = cur.fetchone()[0]

            # Get column info
            cur.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            columns_info = cur.fetchall()

            columns = []
            for col_name, col_type in columns_info:
                # Get sample values
                cur.execute(f"""
                    SELECT DISTINCT "{col_name}"
                    FROM {full_table}
                    WHERE "{col_name}" IS NOT NULL
                    LIMIT 10
                """)
                sample_values = [row[0] for row in cur.fetchall()]

                description = infer_column_description(col_name, sample_values)

                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'description': description,
                    'sample_values': sample_values[:5],  # Limit for display
                })

    return {
        'table_name': table_name,
        'schema': schema,
        'row_count': row_count,
        'columns': columns,
    }


def get_all_tables_metadata(schema: str = 'staging') -> List[Dict]:
    """Get metadata for all tables in a schema."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (schema,))
            tables = [row[0] for row in cur.fetchall()]

    return [get_table_metadata(t, schema) for t in tables]
