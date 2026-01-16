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
