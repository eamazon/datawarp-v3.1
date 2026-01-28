"""
DataWarp CLI module - shared console and utilities.
"""
from datawarp.cli.console import console, custom_theme
from datawarp.cli.helpers import (
    group_files_by_period,
    infer_sheet_description,
    extract_name_from_url,
    make_filename_pattern,
)
from datawarp.cli.file_processor import (
    process_data_file,
    load_period_files,
)
from datawarp.cli.sheet_selector import (
    analyze_sheets,
    display_sheet_table,
    select_sheets,
)
from datawarp.cli.bootstrap import bootstrap_command
from datawarp.cli.scan import scan_command

__all__ = [
    'console',
    'custom_theme',
    'group_files_by_period',
    'infer_sheet_description',
    'extract_name_from_url',
    'make_filename_pattern',
    'process_data_file',
    'load_period_files',
    'analyze_sheets',
    'display_sheet_table',
    'select_sheets',
    'bootstrap_command',
    'scan_command',
]
