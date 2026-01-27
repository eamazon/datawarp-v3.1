"""Pipeline configuration dataclasses"""
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
import json


@dataclass
class SheetMapping:
    """Mapping from Excel sheet to database table"""
    sheet_pattern: str              # Sheet name or regex pattern
    table_name: str                 # Target table: "tbl_adhd_icb"
    column_mappings: Dict[str, str] = field(default_factory=dict)  # source -> canonical
    column_types: Dict[str, str] = field(default_factory=dict)     # canonical -> pg_type

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'SheetMapping':
        return cls(**data)


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

    def to_dict(self) -> dict:
        return {
            'pipeline_id': self.pipeline_id,
            'name': self.name,
            'landing_page': self.landing_page,
            'file_patterns': [f.to_dict() for f in self.file_patterns],
            'loaded_periods': self.loaded_periods,
            'auto_load': self.auto_load,
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
