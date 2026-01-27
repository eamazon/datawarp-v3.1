"""Name sanitization for PostgreSQL identifiers"""
import re
from typing import Optional


def sanitize_name(name: str) -> str:
    """
    Convert a string to a PostgreSQL-safe identifier.

    - Lowercase
    - Replace spaces/special chars with underscores
    - Remove consecutive underscores
    - Strip leading/trailing underscores
    - Truncate to 63 chars (PostgreSQL limit)
    """
    if not name:
        return "unnamed"

    # Lowercase
    result = name.lower()

    # Replace common separators with underscore
    result = re.sub(r'[\s\-./\\()]+', '_', result)

    # Remove any remaining non-alphanumeric (except underscore)
    result = re.sub(r'[^a-z0-9_]', '', result)

    # Collapse multiple underscores
    result = re.sub(r'_+', '_', result)

    # Strip leading/trailing underscores
    result = result.strip('_')

    # Ensure starts with letter (PostgreSQL requirement)
    if result and not result[0].isalpha():
        result = 'c_' + result

    # Truncate to PostgreSQL limit
    if len(result) > 63:
        result = result[:63].rstrip('_')

    return result or "unnamed"


def make_table_name(pipeline_id: str, sheet_name: str) -> str:
    """
    Create a table name from pipeline ID and sheet name.

    Example: make_table_name("adhd", "ICB Level Data") -> "tbl_adhd_icb_level_data"
    """
    pipeline_clean = sanitize_name(pipeline_id)
    sheet_clean = sanitize_name(sheet_name)

    # Combine with prefix
    name = f"tbl_{pipeline_clean}_{sheet_clean}"

    # Truncate if needed
    if len(name) > 63:
        name = name[:63].rstrip('_')

    return name


def make_pipeline_id(name: str) -> str:
    """
    Create a pipeline ID from a publication name.

    Example: make_pipeline_id("MI ADHD Data") -> "mi_adhd_data"
    """
    return sanitize_name(name)
