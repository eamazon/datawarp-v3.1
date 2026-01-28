"""Metadata inference using heuristics and LLM enrichment"""
from .inference import infer_column_description, infer_entity_type, get_table_metadata, get_all_tables_metadata
from .grain import detect_grain, ENTITY_PATTERNS
from .enrich import enrich_sheet
from .file_context import FileContext, extract_metadata_text, extract_file_context
