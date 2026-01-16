"""CLI for the data ingestion tool.

Usage:
    # Run manifest-driven ingestion
    python3 -m src.cli run --manifest manifests/phase1.yaml
    
    # Legacy: query-based ingestion (still supported)
    python3 -m src.cli fetch data.gov --query "health" --limit 5
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import List

from src.normalize import normalize_dataset, normalize_fred_series, resources_to_json
from src.sources import data_gov, fred
from src.storage import ensure_dir
from src import downloader


def _write_json(output_path: str, datasets: List[dict]) -> None:
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(datasets, handle, ensure_ascii=False, indent=2)


def _write_csv(output_path: str, datasets: List[dict]) -> None:
    with open(output_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["title", "description", "resources"])
        writer.writeheader()
        for dataset in datasets:
            writer.writerow({
                "title": dataset.get("title"),
                "description": dataset.get("description"),
                "resources": resources_to_json(dataset.get("resources", [])),
            })


def _output_filename(source: str, output_format: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{source}_datasets_{timestamp}.{output_format}"


def _progress_callback(result: dict) -> None:
    if result.get("skipped"):
        print(f"  [SKIP] {result.get('path')} (already exists)")
    elif result.get("success"):
        size = result.get("size_bytes", 0)
        print(f"  [OK] {result.get('path')} ({size:,} bytes)")
    else:
        print(f"  [FAIL] {result.get('url')}: {result.get('error')}")


def cmd_run(args: argparse.Namespace) -> int:
    """Run manifest-driven ingestion."""
    from src.runner import run_manifest
    
    api_keys = {}
    if args.api_key:
        # Assume FRED for now
        api_keys["fred"] = args.api_key
    
    try:
        report = run_manifest(
            args.manifest,
            data_dir=args.data_dir,
            api_keys=api_keys,
            dry_run=args.dry_run,
            force=args.force,
        )
        
        print(f"\n{'='*50}")
        print(f"Run ID: {report.run_id}")
        print(f"Status: {report.status}")
        print(f"Datasets: {report.succeeded} succeeded, {report.skipped} skipped, {report.failed} failed")
        
        if report.failed > 0:
            print("\nFailed datasets:")
            for r in report.results:
                if r.status == "failed":
                    print(f"  - {r.dataset_id}: {r.error}")
        
        return 0 if report.failed == 0 else 1
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_status(args: argparse.Namespace) -> int:
    """Show status of local store."""
    from src.store import LocalStore
    
    try:
        store = LocalStore(args.data_dir)
        
        print(f"Data directory: {store.base_dir}")
        print(f"Store type: {store.store_type}")
        print()
        
        # Show datasets
        datasets = store.get_datasets()
        if datasets:
            print(f"Datasets ({len(datasets)}):")
            for ds in datasets:
                print(f"  - {ds['id']} ({ds['source']}) - last updated: {ds.get('last_updated', 'never')}")
        else:
            print("No datasets ingested yet.")
        
        print()
        
        # Show recent runs
        runs = store.get_runs(limit=5)
        if runs:
            print(f"Recent runs:")
            for run in runs:
                print(f"  - {run['run_id']} ({run['manifest_name']}) - {run['status']} - {run['started_at']}")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_fetch(args: argparse.Namespace) -> int:
    """Legacy fetch command for backwards compatibility."""
    # Ingest datasets based on source
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
    elif args.source == "fred":
        if args.offline and not args.cache_dir:
            raise ValueError("--offline requires --cache-dir")
        datasets = [
            normalize_fred_series(series)
            for series in fred.iter_series(
                query=args.query,
                limit=args.limit,
                api_key=args.api_key,
                cache_dir=args.cache_dir,
                offline=args.offline,
            )
        ]
    else:
        print(f"Unsupported source: {args.source}", file=sys.stderr)
        return 1

    # Write metadata output
    ensure_dir(args.output)
    output_path = os.path.join(args.output, _output_filename(args.source, args.format))

    if args.format == "json":
        _write_json(output_path, datasets)
    else:
        _write_csv(output_path, datasets)

    print(f"Wrote {len(datasets)} datasets to {output_path}")
    
    # Download resources if requested
    if args.download_resources:
        print(f"\nDownloading resources to {args.download_dir}...")
        
        if args.source == "fred" and args.download_observations:
            result = downloader.download_fred_observations(
                datasets,
                args.download_dir,
                api_key=args.api_key or os.environ.get("FRED_API_KEY", ""),
                format="json",
            )
            print(f"\nDownloaded {result['success']} observation files, {result['failed']} failed")
        else:
            result = downloader.download_resources(
                datasets,
                args.download_dir,
                source=args.source.replace(".", "_"),
                formats=args.download_formats,
                concurrency=args.concurrency,
                progress_callback=_progress_callback,
            )
            print(f"\nDownload complete: {result['success']} succeeded, "
                  f"{result['skipped']} skipped, {result['failed']} failed")
    
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Data ingestion tool for government data sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run manifest-driven ingestion
    python3 -m src.cli run --manifest manifests/phase1.yaml
    
    # Check status of local store
    python3 -m src.cli status --data-dir ./data
    
    # Legacy: query-based fetch
    python3 -m src.cli fetch data.gov --query "health" --limit 5
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # ===== RUN command =====
    run_parser = subparsers.add_parser("run", help="Run manifest-driven ingestion")
    run_parser.add_argument("--manifest", "-m", required=True,
                           help="Path to manifest YAML/JSON file")
    run_parser.add_argument("--data-dir", "-d",
                           help="Override output data directory")
    run_parser.add_argument("--api-key",
                           help="API key (or set via env vars)")
    run_parser.add_argument("--dry-run", action="store_true",
                           help="Show what would be fetched without fetching")
    run_parser.add_argument("--force", action="store_true",
                           help="Re-fetch even if content unchanged")
    
    # ===== STATUS command =====
    status_parser = subparsers.add_parser("status", help="Show status of local store")
    status_parser.add_argument("--data-dir", "-d", default="./data",
                              help="Path to data directory")
    
    # ===== FETCH command (legacy) =====
    fetch_parser = subparsers.add_parser("fetch", help="Legacy query-based fetch")
    fetch_parser.add_argument("source", choices=["data.gov", "fred"],
                             help="Source to fetch from")
    fetch_parser.add_argument("--query", default="*:*",
                             help="Search query")
    fetch_parser.add_argument("--limit", type=int, default=25,
                             help="Max datasets to fetch")
    fetch_parser.add_argument("--output", default="output",
                             help="Output directory")
    fetch_parser.add_argument("--format", choices=["json", "csv"], default="json",
                             help="Output format")
    fetch_parser.add_argument("--cache-dir",
                             help="Cache directory for offline mode")
    fetch_parser.add_argument("--offline", action="store_true",
                             help="Use cached data only")
    fetch_parser.add_argument("--api-key",
                             help="API key for the source")
    fetch_parser.add_argument("--download-resources", action="store_true",
                             help="Download resource files")
    fetch_parser.add_argument("--download-dir", default="downloads",
                             help="Directory for downloads")
    fetch_parser.add_argument("--download-formats", nargs="+",
                             help="Formats to download")
    fetch_parser.add_argument("--concurrency", type=int, default=4,
                             help="Parallel downloads")
    fetch_parser.add_argument("--download-observations", action="store_true",
                             help="(FRED) Download observations")
    
    args = parser.parse_args()
    
    if args.command == "run":
        return cmd_run(args)
    elif args.command == "status":
        return cmd_status(args)
    elif args.command == "fetch":
        return cmd_fetch(args)
    else:
        # No command specified - show help
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
