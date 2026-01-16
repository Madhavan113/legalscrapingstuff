import os
from datetime import datetime
from typing import Any, Dict, List

from src import config as config_loader
from src.normalize import normalize_dataset, resources_to_json
from src.sources import data_gov
from src.storage import ensure_dir


def _output_filename(source: str, output_format: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return f"{source}_datasets_{timestamp}.{output_format}"


def _write_json(output_path: str, datasets: List[dict]) -> None:
    import json

    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(datasets, handle, ensure_ascii=False, indent=2)


def _write_csv(output_path: str, datasets: List[dict]) -> None:
    import csv

    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["title", "description", "resources"])
        writer.writeheader()
        for dataset in datasets:
            writer.writerow(
                {
                    "title": dataset.get("title"),
                    "description": dataset.get("description"),
                    "resources": resources_to_json(dataset.get("resources", [])),
                }
            )


def run_ingestion(config: Dict[str, Any]) -> str:
    source = config["source"]
    query = config.get("query", "*:*")
    limit = config.get("limit", 25)
    output_dir = config["output"]
    output_format = config["format"]
    cache_dir = config.get("cache_dir")
    offline = config.get("offline", False)

    if source == "data.gov":
        if offline and not cache_dir:
            raise ValueError("offline ingestion requires cache_dir")
        datasets = [
            normalize_dataset(dataset)
            for dataset in data_gov.iter_datasets(
                query=query,
                limit=limit,
                cache_dir=cache_dir,
                offline=offline,
            )
        ]
    else:
        raise ValueError(f"Unsupported source: {source}")

    ensure_dir(output_dir)
    output_path = os.path.join(output_dir, _output_filename(source, output_format))

    if output_format == "json":
        _write_json(output_path, datasets)
    elif output_format == "csv":
        _write_csv(output_path, datasets)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    return output_path


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run ingestion from a config file.")
    parser.add_argument("config", help="Path to JSON config file.")
    args = parser.parse_args()

    config = config_loader.load_config(args.config)
    output_path = run_ingestion(config)
    print(f"Wrote datasets to {output_path}")


if __name__ == "__main__":
    main()
