"""LLM enrichment for semantic column names and descriptions using LiteLLM/Gemini"""
import json
import os
import time
from typing import Dict, List, Optional

from dotenv import load_dotenv

from .column_compressor import compress_columns, expand_columns
from .canonicalize import remove_date_patterns

load_dotenv()


def enrich_sheet(
    sheet_name: str,
    columns: List[str],
    sample_rows: List[Dict],
    publication_hint: str = "",
    grain_hint: str = "",
    pipeline_id: str = "",
    source_file: str = "",
    file_context: Optional[Dict] = None,
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
        file_context: Structured context from metadata sheets (sheets, kpis, methodology)

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
        from litellm import completion, completion_cost
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

    # Build context section from file_context (extracted from metadata sheets)
    context_section = ""
    if file_context:
        # Sheet description from Contents/TOC
        sheet_desc = file_context.get('sheets', {}).get(sheet_name, '')
        if sheet_desc:
            context_section += f"\nSheet purpose (from file contents): {sheet_desc}"
        # KPI definitions from Notes/Definitions
        kpis = file_context.get('kpis', {})
        if kpis:
            context_section += f"\nKPI definitions from file: {json.dumps(kpis)}"
        # Methodology notes
        methodology = file_context.get('methodology', '')
        if methodology:
            context_section += f"\nMethodology: {methodology}"

    # Compress timeseries columns to reduce tokens
    original_column_count = len(columns)
    compressed_columns, pattern_info = compress_columns(columns)
    columns_for_prompt = compressed_columns

    # Add pattern context to prompt if columns were compressed
    pattern_note = ""
    if pattern_info:
        pattern_note = f"""

NOTE: This sheet has {pattern_info['count']} timeseries columns following pattern "{pattern_info['pattern']}".
Only {len(compressed_columns)} sample columns shown above. Apply the same semantic naming pattern to all timeseries columns."""

    prompt = f"""You are analyzing an NHS dataset. Suggest SHORT semantic names for this data.
{context_section}
Sheet name: {sheet_name}
Publication context: {publication_hint}{grain_context}
Columns: {columns_for_prompt}
Sample data (first 3 rows):
{json.dumps(sample_rows[:3], indent=2, default=str)}{pattern_note}

Respond with JSON only, no markdown code blocks:
{{
    "table_name": "short_name",
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

CRITICAL Rules for table_name:
- MAXIMUM 30 characters (will be prefixed with tbl_)
- lowercase, snake_case, NO prefix like tbl_
- Keep it SHORT: icb_referrals NOT icb_adhd_referrals_by_month
- Format: {{grain}}_{{metric}} e.g., icb_referrals, trust_waiting, gp_patients
- Examples: icb_referrals (good), integrated_care_board_referral_data (too long)

Column rules:
- Use NHS terminology: icb_code, trust_code, referrals, waiting_list
- lowercase, snake_case
- Descriptions: concise, mention units if applicable"""

    start_time = time.time()
    log_data = {
        'pipeline_id': pipeline_id,
        'source_file': source_file,
        'sheet_name': sheet_name,
        'provider': provider,
        'model': model,
        'prompt_text': prompt,
        'original_column_count': original_column_count,
        'compressed_column_count': len(compressed_columns) if pattern_info else None,
        'pattern_detected': pattern_info['pattern'] if pattern_info else None,
    }

    try:
        response = completion(
            model=model_id,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=int(os.getenv('LLM_MAX_OUTPUT_TOKENS', '2000')),
            temperature=float(os.getenv('LLM_TEMPERATURE', '0.1')),
            timeout=int(os.getenv('LLM_TIMEOUT', '60'))
        )

        duration_ms = int((time.time() - start_time) * 1000)
        text = response.choices[0].message.content

        # Extract token usage
        usage = getattr(response, 'usage', None)
        if usage:
            log_data['input_tokens'] = getattr(usage, 'prompt_tokens', 0)
            log_data['output_tokens'] = getattr(usage, 'completion_tokens', 0)
            log_data['total_tokens'] = getattr(usage, 'total_tokens', 0)

        # Calculate cost using LiteLLM's pricing database
        try:
            cost = completion_cost(completion_response=response)
            log_data['cost_usd'] = cost
        except Exception:
            # Cost calculation may fail for some models not in pricing DB
            log_data['cost_usd'] = None

        log_data['response_text'] = text
        log_data['duration_ms'] = duration_ms

        # Handle potential markdown code blocks
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]

        result = json.loads(text.strip())

        # Validate structure
        if not all(k in result for k in ['table_name', 'columns', 'descriptions']):
            log_data['success'] = False
            log_data['error_message'] = "Missing required fields in response"
            _log_enrichment_call(log_data)
            print("LLM response missing required fields, using fallback")
            return _fallback_enrichment(sheet_name, columns)

        # Ensure table_description exists
        if 'table_description' not in result:
            result['table_description'] = f"Data from {sheet_name}"

        # Expand columns back to full set if compressed
        if pattern_info:
            result = expand_columns(result, pattern_info)

        # Remove date patterns from table name for cross-period consistency
        if 'table_name' in result:
            result['table_name'] = remove_date_patterns(result['table_name'])

        # Log successful call
        log_data['success'] = True
        log_data['suggested_table_name'] = result['table_name']
        log_data['suggested_columns'] = result['columns']
        _log_enrichment_call(log_data)

        return result

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        log_data['duration_ms'] = duration_ms
        log_data['success'] = False
        log_data['error_message'] = str(e)
        _log_enrichment_call(log_data)
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


def _log_enrichment_call(data: Dict) -> None:
    """Log enrichment API call to database."""
    try:
        from ..storage import get_connection
        import json

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO datawarp.tbl_enrichment_log (
                        pipeline_id, source_file, sheet_name,
                        provider, model,
                        prompt_text, response_text,
                        input_tokens, output_tokens, total_tokens, cost_usd,
                        duration_ms,
                        suggested_table_name, suggested_columns,
                        success, error_message,
                        original_column_count, compressed_column_count, pattern_detected
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    data.get('pipeline_id'),
                    data.get('source_file'),
                    data.get('sheet_name'),
                    data.get('provider'),
                    data.get('model'),
                    data.get('prompt_text'),
                    data.get('response_text'),
                    data.get('input_tokens'),
                    data.get('output_tokens'),
                    data.get('total_tokens'),
                    data.get('cost_usd'),
                    data.get('duration_ms'),
                    data.get('suggested_table_name'),
                    json.dumps(data.get('suggested_columns')) if data.get('suggested_columns') else None,
                    data.get('success', False),
                    data.get('error_message'),
                    data.get('original_column_count'),
                    data.get('compressed_column_count'),
                    data.get('pattern_detected'),
                ))
    except Exception as e:
        # Don't fail the enrichment if logging fails
        print(f"Warning: Failed to log enrichment call: {e}")
