"""Bulk resource downloader for dataset files.

Downloads actual data files (CSV, JSON, etc.) from resource URLs.
Supports parallel downloads, rate limiting, and resume capability.
"""
import json
import os
import re
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional, Set
from urllib.parse import urlparse


def _sanitize_filename(name: str, max_length: int = 100) -> str:
    """Sanitize a string to be used as a filename."""
    # Remove or replace invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Replace whitespace with underscores
    name = re.sub(r'\s+', '_', name)
    # Remove leading/trailing underscores and dots
    name = name.strip('_.')
    # Truncate if too long
    if len(name) > max_length:
        name = name[:max_length]
    return name or "unnamed"


def _get_extension_from_format(format_str: str) -> str:
    """Get file extension from format string."""
    format_lower = format_str.lower().strip()
    
    # Common mappings
    mappings = {
        "csv": "csv",
        "json": "json",
        "xml": "xml",
        "zip": "zip",
        "pdf": "pdf",
        "html": "html",
        "rdf": "rdf",
        "xlsx": "xlsx",
        "xls": "xls",
        "esri rest": "json",  # Esri REST services return JSON
        "geojson": "geojson",
        "kml": "kml",
        "shapefile": "zip",
    }
    
    return mappings.get(format_lower, format_lower or "dat")


def _download_file(
    url: str,
    output_path: str,
    timeout: int = 60,
) -> Dict:
    """Download a single file.
    
    Returns:
        Dict with 'success', 'url', 'path', and optionally 'error'
    """
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "AmethystDataIntegration/1.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            content = response.read()
            
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, "wb") as f:
            f.write(content)
            
        return {
            "success": True,
            "url": url,
            "path": output_path,
            "size_bytes": len(content),
        }
    except urllib.error.HTTPError as e:
        return {
            "success": False,
            "url": url,
            "path": output_path,
            "error": f"HTTP {e.code}: {e.reason}",
        }
    except urllib.error.URLError as e:
        return {
            "success": False,
            "url": url,
            "path": output_path,
            "error": str(e.reason),
        }
    except Exception as e:
        return {
            "success": False,
            "url": url,
            "path": output_path,
            "error": str(e),
        }


def download_resources(
    datasets: List[Dict],
    output_dir: str,
    source: str = "unknown",
    formats: Optional[List[str]] = None,
    concurrency: int = 4,
    rate_limit: float = 0.5,
    resume: bool = True,
    progress_callback: Optional[Callable[[Dict], None]] = None,
) -> Dict:
    """Download resource files from datasets.
    
    Args:
        datasets: List of normalized dataset dicts with 'title' and 'resources'
        output_dir: Directory to save downloaded files
        source: Source name for file naming (e.g., 'data.gov', 'fred')
        formats: List of formats to download (e.g., ['CSV', 'JSON']), None = all
        concurrency: Number of parallel downloads
        rate_limit: Minimum seconds between starting downloads
        resume: Skip files that already exist
        progress_callback: Called with result dict for each download
        
    Returns:
        Summary dict with 'total', 'success', 'skipped', 'failed', 'results'
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Normalize format filter to lowercase
    format_filter: Optional[Set[str]] = None
    if formats:
        format_filter = {f.lower() for f in formats}
    
    # Build list of downloads
    downloads = []
    for dataset_idx, dataset in enumerate(datasets):
        title = dataset.get("title", f"dataset_{dataset_idx}")
        safe_title = _sanitize_filename(title)
        
        for res_idx, resource in enumerate(dataset.get("resources", [])):
            url = resource.get("url")
            if not url:
                continue
                
            res_format = resource.get("format", "")
            if format_filter and res_format.lower() not in format_filter:
                continue
            
            ext = _get_extension_from_format(res_format)
            filename = f"{source}_{safe_title}_{res_idx}.{ext}"
            output_path = os.path.join(output_dir, filename)
            
            downloads.append({
                "url": url,
                "output_path": output_path,
                "format": res_format,
                "title": title,
            })
    
    # Execute downloads
    results = []
    skipped = 0
    success = 0
    failed = 0
    
    def do_download(item: Dict) -> Dict:
        """Download with rate limiting."""
        time.sleep(rate_limit)  # Simple rate limiting
        return _download_file(item["url"], item["output_path"])
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {}
        
        for item in downloads:
            # Check if file exists (resume support)
            if resume and os.path.exists(item["output_path"]):
                result = {
                    "success": True,
                    "url": item["url"],
                    "path": item["output_path"],
                    "skipped": True,
                }
                results.append(result)
                skipped += 1
                if progress_callback:
                    progress_callback(result)
                continue
            
            future = executor.submit(do_download, item)
            futures[future] = item
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            
            if result.get("success"):
                success += 1
            else:
                failed += 1
                
            if progress_callback:
                progress_callback(result)
    
    return {
        "total": len(downloads),
        "success": success,
        "skipped": skipped,
        "failed": failed,
        "results": results,
    }


def download_fred_observations(
    series_list: List[Dict],
    output_dir: str,
    api_key: str,
    format: str = "json",
) -> Dict:
    """Download FRED observations for a list of series.
    
    Args:
        series_list: List of FRED series dicts with 'id' key
        output_dir: Directory to save files
        api_key: FRED API key
        format: Output format ('json' or 'csv')
        
    Returns:
        Summary dict
    """
    from src.sources import fred
    
    os.makedirs(output_dir, exist_ok=True)
    
    results = []
    success = 0
    failed = 0
    
    for series in series_list:
        series_id = series.get("id") or series.get("series_id")
        if not series_id:
            continue
            
        try:
            observations = fred.get_observations(series_id, api_key=api_key)
            
            if format == "csv":
                filename = f"fred_{series_id}_observations.csv"
                output_path = os.path.join(output_dir, filename)
                with open(output_path, "w") as f:
                    f.write("date,value\n")
                    for obs in observations:
                        f.write(f"{obs.get('date')},{obs.get('value')}\n")
            else:
                filename = f"fred_{series_id}_observations.json"
                output_path = os.path.join(output_dir, filename)
                with open(output_path, "w") as f:
                    json.dump(observations, f, indent=2)
            
            results.append({
                "success": True,
                "series_id": series_id,
                "path": output_path,
                "observation_count": len(observations),
            })
            success += 1
            
        except Exception as e:
            results.append({
                "success": False,
                "series_id": series_id,
                "error": str(e),
            })
            failed += 1
    
    return {
        "total": len(series_list),
        "success": success,
        "failed": failed,
        "results": results,
    }
