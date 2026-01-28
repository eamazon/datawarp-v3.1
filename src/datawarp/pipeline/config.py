"""Pipeline configuration dataclasses"""
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
import json


@dataclass
class SheetMapping:
    """Mapping from Excel sheet to database table"""
    sheet_pattern: str              # Sheet name or regex pattern
    table_name: str                 # Target table: "tbl_adhd_icb"
    table_description: str = ""     # "ADHD referrals by ICB"
    column_mappings: Dict[str, str] = field(default_factory=dict)  # source -> canonical
    column_descriptions: Dict[str, str] = field(default_factory=dict)  # canonical -> description
    column_types: Dict[str, str] = field(default_factory=dict)     # canonical -> pg_type
    grain: str = "unknown"          # "icb", "trust", "national", "unknown"
    grain_column: Optional[str] = None  # which column has the entity
    grain_description: str = ""     # "ICB level data"
    # Version tracking for incremental enrichment
    mappings_version: int = 1       # Bumped when columns added/enriched
    last_enriched: Optional[str] = None  # ISO timestamp of last enrichment

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'SheetMapping':
        # Handle older configs without new fields
        return cls(
            sheet_pattern=data.get('sheet_pattern', ''),
            table_name=data.get('table_name', ''),
            table_description=data.get('table_description', ''),
            column_mappings=data.get('column_mappings', {}),
            column_descriptions=data.get('column_descriptions', {}),
            column_types=data.get('column_types', {}),
            grain=data.get('grain', 'unknown'),
            grain_column=data.get('grain_column'),
            grain_description=data.get('grain_description', ''),
            mappings_version=data.get('mappings_version', 1),
            last_enriched=data.get('last_enriched'),
        )


@dataclass
class FilePattern:
    """Pattern for matching files in a pipeline"""
    filename_pattern: str           # Regex: r"ADHD-.*\.xlsx"
    file_types: List[str] = field(default_factory=lambda: ['xlsx'])
    sheet_mappings: List[SheetMapping] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'filename_pattern': self.filename_pattern,
            'file_types': self.file_types,
            'sheet_mappings': [s.to_dict() for s in self.sheet_mappings],
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'FilePattern':
        return cls(
            filename_pattern=data['filename_pattern'],
            file_types=data.get('file_types', ['xlsx']),
            sheet_mappings=[SheetMapping.from_dict(s) for s in data.get('sheet_mappings', [])],
        )


@dataclass
class PipelineConfig:
    """Complete pipeline configuration"""
    pipeline_id: str                # "adhd"
    name: str                       # "ADHD Referrals"
    landing_page: str               # NHS URL
    file_patterns: List[FilePattern] = field(default_factory=list)
    loaded_periods: List[str] = field(default_factory=list)  # ["2024-11", "2024-12"]
    auto_load: bool = False
    # Discovery configuration
    discovery_mode: str = 'discover'  # template, discover, explicit
    url_pattern: Optional[str] = None  # For template mode: '{landing_page}/{month_name}-{year}'
    frequency: str = 'monthly'  # monthly, quarterly, annual

    def to_dict(self) -> dict:
        return {
            'pipeline_id': self.pipeline_id,
            'name': self.name,
            'landing_page': self.landing_page,
            'file_patterns': [f.to_dict() for f in self.file_patterns],
            'loaded_periods': self.loaded_periods,
            'auto_load': self.auto_load,
            'discovery_mode': self.discovery_mode,
            'url_pattern': self.url_pattern,
            'frequency': self.frequency,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict) -> 'PipelineConfig':
        return cls(
            pipeline_id=data['pipeline_id'],
            name=data['name'],
            landing_page=data['landing_page'],
            file_patterns=[FilePattern.from_dict(f) for f in data.get('file_patterns', [])],
            loaded_periods=data.get('loaded_periods', []),
            auto_load=data.get('auto_load', False),
            discovery_mode=data.get('discovery_mode', 'discover'),
            url_pattern=data.get('url_pattern'),
            frequency=data.get('frequency', 'monthly'),
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'PipelineConfig':
        return cls.from_dict(json.loads(json_str))

    def add_period(self, period: str) -> None:
        """Mark a period as loaded."""
        if period not in self.loaded_periods:
            self.loaded_periods.append(period)
            self.loaded_periods.sort()

    def get_new_periods(self, available_periods: List[str]) -> List[str]:
        """Find periods that haven't been loaded yet."""
        return [p for p in available_periods if p not in self.loaded_periods]
