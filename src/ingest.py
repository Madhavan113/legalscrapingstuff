import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from src import config as config_loader
from src.normalize import normalize_dataset, normalize_fred_series, resources_to_json
from src.sources import data_gov
from src.sources import fred
from src.storage import ensure_dir
from src import downloader


def _output_filename(source: str, output_format: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
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


def run_ingestion(config: Dict[str, Any]) -> Dict[str, Any]:
    """Run ingestion based on config.
    
    Args:
        config: Configuration dict with source, query, limit, etc.
        
    Returns:
        Dict with output_path, datasets, and optional download_result
    """
    source = config["source"]
    query = config.get("query", "*:*")
    limit = config.get("limit", 25)
    output_dir = config["output"]
    output_format = config["format"]
    cache_dir = config.get("cache_dir")
    offline = config.get("offline", False)
    api_key = config.get("api_key")

    # Ingest datasets based on source
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
    elif source == "fred":
        if offline and not cache_dir:
            raise ValueError("offline ingestion requires cache_dir")
        datasets = [
            normalize_fred_series(series)
            for series in fred.iter_series(
                query=query,
                limit=limit,
                api_key=api_key,
                cache_dir=cache_dir,
                offline=offline,
            )
        ]
    else:
        raise ValueError(f"Unsupported source: {source}")

    # Write output
    ensure_dir(output_dir)
    output_path = os.path.join(output_dir, _output_filename(source, output_format))

    if output_format == "json":
        _write_json(output_path, datasets)
    elif output_format == "csv":
        _write_csv(output_path, datasets)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    result = {
        "output_path": output_path,
        "datasets": datasets,
        "count": len(datasets),
    }

    # Download resources if configured
    if config.get("download_resources"):
        download_dir = config.get("download_dir", "downloads")
        download_formats = config.get("download_formats")
        concurrency = config.get("concurrency", 4)
        
        if source == "fred" and config.get("download_observations"):
            download_result = downloader.download_fred_observations(
                datasets,
                download_dir,
                api_key=api_key or os.environ.get("FRED_API_KEY", ""),
                format=config.get("observation_format", "json"),
            )
        else:
            download_result = downloader.download_resources(
                datasets,
                download_dir,
                source=source.replace(".", "_"),
                formats=download_formats,
                concurrency=concurrency,
            )
        
        result["download_result"] = download_result

    return result


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run ingestion from a config file.")
    parser.add_argument("config", help="Path to JSON config file.")
    args = parser.parse_args()

    config = config_loader.load_config(args.config)
    result = run_ingestion(config)
    print(f"Wrote {result['count']} datasets to {result['output_path']}")
    
    if "download_result" in result:
        dr = result["download_result"]
        print(f"Downloads: {dr['success']} succeeded, {dr.get('skipped', 0)} skipped, {dr['failed']} failed")


if __name__ == "__main__":
    main()
