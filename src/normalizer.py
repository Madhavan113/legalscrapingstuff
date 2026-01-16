"""Data normalization and schema inference."""
import csv
import io
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class NormalizedResult:
    """Result of normalizing a dataset."""
    data: List[Dict[str, Any]]
    schema: Dict[str, Any]
    row_count: int
    warnings: List[str] = field(default_factory=list)


def _snake_case(name: str) -> str:
    """Convert string to snake_case."""
    # Replace spaces and special chars with underscores
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    # Convert camelCase to snake_case
    name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
    # Collapse multiple underscores
    name = re.sub(r'_+', '_', name)
    # Lowercase and strip
    return name.lower().strip('_')


def _infer_type(value: Any) -> str:
    """Infer the type of a value."""
    if value is None or value == "":
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        # Try to detect dates
        if re.match(r'^\d{4}-\d{2}-\d{2}', value):
            return "date"
        # Try to detect numbers
        try:
            float(value.replace(",", ""))
            return "number"
        except ValueError:
            pass
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def _coerce_value(value: Any, target_type: str) -> Any:
    """Coerce a value to the target type."""
    if value is None or value == "":
        return None
    
    if target_type == "integer":
        try:
            return int(float(str(value).replace(",", "")))
        except (ValueError, TypeError):
            return None
    
    if target_type == "number":
        try:
            return float(str(value).replace(",", ""))
        except (ValueError, TypeError):
            return None
    
    if target_type == "boolean":
        if isinstance(value, bool):
            return value
        if str(value).lower() in ("true", "yes", "1", "y"):
            return True
        if str(value).lower() in ("false", "no", "0", "n"):
            return False
        return None
    
    if target_type == "date":
        # Return as ISO string
        if isinstance(value, str):
            # Already a date string, normalize it
            return value[:10] if len(value) >= 10 else value
        return str(value)
    
    return value


def infer_schema(data: List[Dict]) -> Dict[str, Any]:
    """Infer schema from a list of records."""
    if not data:
        return {"fields": [], "row_count": 0}
    
    field_types: Dict[str, Dict[str, int]] = {}
    
    for record in data:
        for key, value in record.items():
            if key not in field_types:
                field_types[key] = {}
            
            vtype = _infer_type(value)
            field_types[key][vtype] = field_types[key].get(vtype, 0) + 1
    
    # Determine best type for each field
    fields = []
    for name, type_counts in field_types.items():
        # Remove nulls from consideration for primary type
        non_null_types = {k: v for k, v in type_counts.items() if k != "null"}
        
        if non_null_types:
            primary_type = max(non_null_types, key=non_null_types.get)
        else:
            primary_type = "string"
        
        nullable = "null" in type_counts
        
        fields.append({
            "name": name,
            "type": primary_type,
            "nullable": nullable,
        })
    
    return {
        "fields": fields,
        "row_count": len(data),
    }


def normalize_csv(content: bytes, encoding: str = "utf-8") -> NormalizedResult:
    """Parse and normalize CSV content."""
    warnings = []
    
    # Decode content
    try:
        text = content.decode(encoding)
    except UnicodeDecodeError:
        text = content.decode("latin-1")
        warnings.append(f"Fell back to latin-1 encoding")
    
    # Parse CSV
    reader = csv.DictReader(io.StringIO(text))
    
    # Normalize column names
    if reader.fieldnames:
        column_map = {name: _snake_case(name) for name in reader.fieldnames}
    else:
        column_map = {}
    
    # Read and normalize rows
    data = []
    for row in reader:
        normalized_row = {}
        for orig_name, value in row.items():
            new_name = column_map.get(orig_name, _snake_case(orig_name))
            normalized_row[new_name] = value if value != "" else None
        data.append(normalized_row)
    
    # Infer schema
    schema = infer_schema(data)
    
    # Coerce types based on schema
    type_map = {f["name"]: f["type"] for f in schema["fields"]}
    for row in data:
        for key in row:
            if key in type_map:
                row[key] = _coerce_value(row[key], type_map[key])
    
    return NormalizedResult(
        data=data,
        schema=schema,
        row_count=len(data),
        warnings=warnings,
    )


def normalize_json(content: bytes) -> NormalizedResult:
    """Parse and normalize JSON content."""
    warnings = []
    
    parsed = json.loads(content.decode("utf-8"))
    
    # Handle different JSON structures
    if isinstance(parsed, list):
        data = parsed
    elif isinstance(parsed, dict):
        # Look for common data keys
        for key in ["data", "results", "records", "items", "observations", "seriess"]:
            if key in parsed and isinstance(parsed[key], list):
                data = parsed[key]
                break
        else:
            # Wrap single object
            data = [parsed]
    else:
        data = [{"value": parsed}]
    
    # Normalize column names in each record
    normalized_data = []
    for record in data:
        if isinstance(record, dict):
            normalized_record = {
                _snake_case(k): v for k, v in record.items()
            }
            normalized_data.append(normalized_record)
        else:
            normalized_data.append({"value": record})
    
    # Infer schema
    schema = infer_schema(normalized_data)
    
    return NormalizedResult(
        data=normalized_data,
        schema=schema,
        row_count=len(normalized_data),
        warnings=warnings,
    )


def normalize_content(
    content: bytes,
    format: str,
) -> NormalizedResult:
    """Normalize content based on format."""
    format_lower = format.lower()
    
    if format_lower == "csv":
        return normalize_csv(content)
    elif format_lower in ("json", "fred_series"):
        return normalize_json(content)
    else:
        # Try JSON first, then CSV
        try:
            return normalize_json(content)
        except Exception:
            try:
                return normalize_csv(content)
            except Exception:
                # Return as-is
                return NormalizedResult(
                    data=[{"raw_content": content.decode("utf-8", errors="replace")}],
                    schema={"fields": [{"name": "raw_content", "type": "string"}]},
                    row_count=1,
                    warnings=["Could not parse content, stored as raw"],
                )
