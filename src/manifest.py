"""Manifest parsing and validation for ingestion runs."""
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class DatasetSpec:
    """Specification for a single dataset to ingest."""
    id: str
    source: str  # data.gov, fred, etc.
    
    # Source-specific identifiers (use one)
    dataset_id: Optional[str] = None  # For data.gov
    series_id: Optional[str] = None   # For FRED
    query: Optional[str] = None       # For search-based fetching
    
    # Metadata
    format: str = "JSON"
    update_cadence: Optional[str] = None  # daily, weekly, monthly, yearly
    license: Optional[str] = None
    description: Optional[str] = None
    
    # Optional field hints
    fields_of_interest: List[str] = field(default_factory=list)


@dataclass 
class ManifestSettings:
    """Settings for an ingestion manifest."""
    output_dir: str = "./data"
    store_type: str = "duckdb"  # duckdb, sqlite, files
    log_level: str = "INFO"
    api_keys: Dict[str, str] = field(default_factory=dict)


@dataclass
class Manifest:
    """A complete ingestion manifest."""
    name: str
    description: str
    sources: List[DatasetSpec]
    settings: ManifestSettings
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Manifest":
        """Parse manifest from dictionary."""
        sources = []
        for src in data.get("sources", []):
            sources.append(DatasetSpec(
                id=src["id"],
                source=src["source"],
                dataset_id=src.get("dataset_id"),
                series_id=src.get("series_id"),
                query=src.get("query"),
                format=src.get("format", "JSON"),
                update_cadence=src.get("update_cadence"),
                license=src.get("license"),
                description=src.get("description"),
                fields_of_interest=src.get("fields_of_interest", []),
            ))
        
        settings_data = data.get("settings", {}) or {}
        settings = ManifestSettings(
            output_dir=settings_data.get("output_dir", "./data"),
            store_type=settings_data.get("store_type", "duckdb"),
            log_level=settings_data.get("log_level", "INFO"),
            api_keys=settings_data.get("api_keys") or {},
        )
        
        return cls(
            name=data.get("name", "unnamed"),
            description=data.get("description", ""),
            sources=sources,
            settings=settings,
        )
    
    @classmethod
    def from_yaml(cls, path: str) -> "Manifest":
        """Load manifest from YAML file."""
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)
    
    @classmethod
    def from_json(cls, path: str) -> "Manifest":
        """Load manifest from JSON file."""
        import json
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data)
    
    @classmethod
    def load(cls, path: str) -> "Manifest":
        """Load manifest from file (auto-detect format)."""
        if path.endswith(".yaml") or path.endswith(".yml"):
            return cls.from_yaml(path)
        elif path.endswith(".json"):
            return cls.from_json(path)
        else:
            # Try YAML first
            try:
                return cls.from_yaml(path)
            except Exception:
                return cls.from_json(path)
    
    def validate(self) -> List[str]:
        """Validate manifest, return list of errors."""
        errors = []
        
        if not self.name:
            errors.append("Manifest must have a name")
        
        if not self.sources:
            errors.append("Manifest must have at least one source")
        
        seen_ids = set()
        for spec in self.sources:
            if not spec.id:
                errors.append(f"Dataset spec missing id")
            elif spec.id in seen_ids:
                errors.append(f"Duplicate dataset id: {spec.id}")
            else:
                seen_ids.add(spec.id)
            
            if not spec.source:
                errors.append(f"Dataset {spec.id} missing source")
            
            # Validate source has identifier
            if spec.source == "fred" and not spec.series_id:
                errors.append(f"FRED dataset {spec.id} requires series_id")
            elif spec.source == "data.gov" and not (spec.dataset_id or spec.query):
                errors.append(f"data.gov dataset {spec.id} requires dataset_id or query")
        
        return errors
