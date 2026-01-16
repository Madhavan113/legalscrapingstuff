import json
from typing import Dict, Iterable, List


def normalize_dataset(dataset: Dict) -> Dict:
    resources: List[Dict] = []
    for resource in dataset.get("resources", []):
        resources.append(
            {
                "url": resource.get("url"),
                "format": resource.get("format"),
            }
        )

    return {
        "title": dataset.get("title"),
        "description": dataset.get("notes"),
        "resources": resources,
    }


def normalize_datasets(datasets: Iterable[Dict]) -> List[Dict]:
    return [normalize_dataset(dataset) for dataset in datasets]


def resources_to_json(resources: List[Dict]) -> str:
    return json.dumps(resources, ensure_ascii=False)


def normalize_fred_series(series: Dict) -> Dict:
    """Normalize a FRED series to the standard dataset schema.
    
    Args:
        series: Raw FRED series dict from API
        
    Returns:
        Normalized dataset dict with title, description, resources
    """
    series_id = series.get("id", "")
    
    return {
        "title": series.get("title"),
        "description": series.get("notes"),
        "source": "fred",
        "series_id": series_id,
        "resources": [{
            "url": f"https://fred.stlouisfed.org/series/{series_id}",
            "format": "FRED_SERIES",
            "series_id": series_id,
            "frequency": series.get("frequency"),
            "units": series.get("units"),
            "observation_start": series.get("observation_start"),
            "observation_end": series.get("observation_end"),
        }],
        "metadata": {
            "frequency": series.get("frequency"),
            "frequency_short": series.get("frequency_short"),
            "units": series.get("units"),
            "units_short": series.get("units_short"),
            "seasonal_adjustment": series.get("seasonal_adjustment"),
            "last_updated": series.get("last_updated"),
            "popularity": series.get("popularity"),
        },
    }


def normalize_fred_datasets(series_list: Iterable[Dict]) -> List[Dict]:
    """Normalize a list of FRED series."""
    return [normalize_fred_series(s) for s in series_list]
