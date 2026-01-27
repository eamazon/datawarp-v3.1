"""Pipeline configuration and management"""
from .config import PipelineConfig, FilePattern, SheetMapping
from .repository import save_config, load_config, list_configs, record_load
