"""LLM enrichment for semantic column names and descriptions using LiteLLM/Gemini"""
import json
import os
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()


def enrich_sheet(
    sheet_name: str,
    columns: List[str],
    sample_rows: List[Dict],
    publication_hint: str = "",
    grain_hint: str = ""
) -> Dict:
    """
    Call LLM API to get semantic names and descriptions.

    Uses LiteLLM with Gemini (configurable via LLM_PROVIDER env var).

    Args:
        sheet_name: Original sheet name
        columns: List of column headers (already sanitized)
        sample_rows: First 3-5 rows as dicts
        publication_hint: e.g., "ADHD referrals", "MSA breaches"
        grain_hint: e.g., "icb", "trust" - from grain detection

    Returns:
        {
            "table_name": "adhd_icb_referrals",
            "table_description": "ADHD referrals by Integrated Care Board",
            "columns": {
                "org_code": "icb_code",
                "measure_1": "referrals_received"
            },
            "descriptions": {
                "icb_code": "Integrated Care Board identifier (e.g., QWE)",
                "referrals_received": "Number of ADHD referrals received in period"
            }
        }
    """
    try:
        from litellm import completion
    except ImportError:
        print("litellm not installed, using fallback")
        return _fallback_enrichment(sheet_name, columns)

    # Build model identifier
    provider = os.getenv('LLM_PROVIDER', 'gemini')
    model = os.getenv('LLM_MODEL', 'gemini-2.0-flash-exp')

    if provider == 'gemini':
        model_id = f"gemini/{model}"
    elif provider == 'openai':
        model_id = model
    elif provider == 'anthropic':
        model_id = model
    else:
        model_id = f"{provider}/{model}"

    grain_context = f"\nData grain: {grain_hint} level data" if grain_hint else ""

    prompt = f"""You are analyzing an NHS dataset. Suggest semantic names for this data.

Sheet name: {sheet_name}
Publication context: {publication_hint}{grain_context}
Columns: {columns}
Sample data (first 3 rows):
{json.dumps(sample_rows[:3], indent=2, default=str)}

Respond with JSON only, no markdown code blocks:
{{
    "table_name": "lowercase_snake_case_descriptive_name",
    "table_description": "One sentence describing what this table contains",
    "columns": {{
        "original_col_name": "semantic_name",
        ...for each column
    }},
    "descriptions": {{
        "semantic_name": "What this column contains",
        ...for each column
    }}
}}

Rules:
- table_name: lowercase, snake_case, no prefix like tbl_
- Use NHS terminology: icb_code, trust_code, referrals, waiting_list
- Column names: lowercase, snake_case
- Descriptions: concise, mention units if applicable
- Include grain in table_name if appropriate (e.g., icb_referrals, trust_waiting)"""

    try:
        response = completion(
            model=model_id,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=int(os.getenv('LLM_MAX_OUTPUT_TOKENS', '2000')),
            temperature=float(os.getenv('LLM_TEMPERATURE', '0.1')),
            timeout=int(os.getenv('LLM_TIMEOUT', '60'))
        )

        text = response.choices[0].message.content

        # Handle potential markdown code blocks
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]

        result = json.loads(text.strip())

        # Validate structure
        if not all(k in result for k in ['table_name', 'columns', 'descriptions']):
            print("LLM response missing required fields, using fallback")
            return _fallback_enrichment(sheet_name, columns)

        # Ensure table_description exists
        if 'table_description' not in result:
            result['table_description'] = f"Data from {sheet_name}"

        return result

    except Exception as e:
        print(f"Enrichment failed: {e}, using fallback")
        return _fallback_enrichment(sheet_name, columns)


def _fallback_enrichment(sheet_name: str, columns: List[str]) -> Dict:
    """Fallback when LLM call fails - return identity mappings."""
    from ..utils.sanitize import sanitize_name

    table_name = sanitize_name(sheet_name)
    return {
        "table_name": table_name,
        "table_description": f"Data from {sheet_name}",
        "columns": {c: c for c in columns},
        "descriptions": {c: "" for c in columns}
    }
