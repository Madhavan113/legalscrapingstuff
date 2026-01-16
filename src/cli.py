import argparse
import csv
import json
import os
from datetime import datetime
from typing import List

from src.normalize import normalize_dataset, resources_to_json
from src.sources import data_gov
from src.storage import ensure_dir


def _write_json(output_path: str, datasets: List[dict]) -> None:
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(datasets, handle, ensure_ascii=False, indent=2)


def _write_csv(output_path: str, datasets: List[dict]) -> None:
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


def _output_filename(source: str, output_format: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return f"{source}_datasets_{timestamp}.{output_format}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest datasets from supported sources.")
    parser.add_argument("source", choices=["data.gov"], help="Source to ingest from.")
    parser.add_argument("--query", default="*:*", help="Search query for the source.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum number of datasets to fetch.")
    parser.add_argument(
        "--output",
        default="output",
        help="Directory to write output files into.",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output file format.",
    )
    parser.add_argument(
        "--cache-dir",
        help="Directory to store/read cached CKAN pages for offline ingestion.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use cached pages only; do not make network calls.",
    )
    args = parser.parse_args()

    if args.source == "data.gov":
        if args.offline and not args.cache_dir:
            raise ValueError("--offline requires --cache-dir")
        datasets = [
            normalize_dataset(dataset)
            for dataset in data_gov.iter_datasets(
                query=args.query,
                limit=args.limit,
                cache_dir=args.cache_dir,
                offline=args.offline,
            )
        ]
    else:
        raise ValueError(f"Unsupported source: {args.source}")

    ensure_dir(args.output)
    output_path = os.path.join(args.output, _output_filename(args.source, args.format))

    if args.format == "json":
        _write_json(output_path, datasets)
    else:
        _write_csv(output_path, datasets)

    print(f"Wrote {len(datasets)} datasets to {output_path}")


if __name__ == "__main__":
    main()
