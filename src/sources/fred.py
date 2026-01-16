"""FRED (Federal Reserve Economic Data) API client.

API Documentation: https://fred.stlouisfed.org/docs/api/fred/
"""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Dict, Generator, List, Optional

from src import storage


FRED_BASE_URL = "https://api.stlouisfed.org/fred"
DEFAULT_PAGE_SIZE = 100  # FRED max is 1000, but we use smaller pages for reliability


def _get_api_key(api_key: Optional[str] = None) -> str:
    """Get API key from argument or environment variable."""
    key = api_key or os.environ.get("FRED_API_KEY")
    if not key:
        raise ValueError(
            "FRED API key required. Set FRED_API_KEY environment variable "
            "or pass --api-key argument."
        )
    return key


def _build_search_url(
    api_key: str,
    search_text: str,
    offset: int,
    limit: int,
) -> str:
    """Build URL for series search endpoint."""
    params = urllib.parse.urlencode({
        "api_key": api_key,
        "file_type": "json",
        "search_text": search_text,
        "search_type": "full_text",
        "offset": offset,
        "limit": limit,
    })
    return f"{FRED_BASE_URL}/series/search?{params}"


def _build_observations_url(
    api_key: str,
    series_id: str,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
) -> str:
    """Build URL for series observations endpoint."""
    params = {
        "api_key": api_key,
        "file_type": "json",
        "series_id": series_id,
    }
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end
    
    return f"{FRED_BASE_URL}/series/observations?{urllib.parse.urlencode(params)}"


def _fetch_json(url: str) -> Dict:
    """Fetch JSON from URL."""
    with urllib.request.urlopen(url) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _iter_cached_pages(cache_dir: str) -> Generator[Dict, None, None]:
    """Iterate over cached pages."""
    pages: List[Dict] = storage.load_cached_pages(cache_dir)
    for page in pages:
        # FRED returns error_code/error_message on failure
        if "error_code" in page:
            raise RuntimeError(f"Cached FRED page has error: {page}")
        yield page


def iter_series(
    query: str,
    limit: Optional[int] = None,
    api_key: Optional[str] = None,
    cache_dir: Optional[str] = None,
    offline: bool = False,
) -> Generator[Dict, None, None]:
    """Iterate over FRED series matching a search query.
    
    Args:
        query: Search text for series (searches title, units, frequency, tags)
        limit: Maximum number of series to return (None = all)
        api_key: FRED API key (or set FRED_API_KEY env var)
        cache_dir: Directory to cache API responses
        offline: Use cached pages only, no network calls
        
    Yields:
        Series dictionaries with keys: id, title, notes, frequency, units, etc.
    """
    fetched = 0
    offset = 0
    page_size = DEFAULT_PAGE_SIZE
    page_index = 0

    if offline:
        if not cache_dir:
            raise ValueError("offline mode requires cache_dir")
        pages = _iter_cached_pages(cache_dir)
    else:
        key = _get_api_key(api_key)
        pages = None

    while True:
        if limit is not None:
            remaining = limit - fetched
            if remaining <= 0:
                break
            page_size = min(page_size, remaining)

        if offline:
            try:
                response = next(pages)  # type: ignore[arg-type]
            except StopIteration:
                break
        else:
            url = _build_search_url(key, search_text=query, offset=offset, limit=page_size)
            response = _fetch_json(url)
            storage.cache_page(cache_dir, f"fred_page_{page_index:04d}.json", response)

        # Check for API errors
        if "error_code" in response:
            raise RuntimeError(f"FRED API error: {response.get('error_message')}")

        series_list = response.get("seriess", [])
        if not series_list:
            break

        for series in series_list:
            yield series
            fetched += 1
            if limit is not None and fetched >= limit:
                return

        offset += len(series_list)
        page_index += 1


def get_observations(
    series_id: str,
    api_key: Optional[str] = None,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    cache_dir: Optional[str] = None,
) -> List[Dict]:
    """Get observation data for a specific series.
    
    Args:
        series_id: FRED series ID (e.g., 'GDP', 'UNRATE')
        api_key: FRED API key
        observation_start: Start date (YYYY-MM-DD format)
        observation_end: End date (YYYY-MM-DD format)
        cache_dir: Directory to cache the response
        
    Returns:
        List of observation dictionaries with 'date' and 'value' keys
    """
    key = _get_api_key(api_key)
    url = _build_observations_url(
        key,
        series_id,
        observation_start=observation_start,
        observation_end=observation_end,
    )
    
    response = _fetch_json(url)
    
    if "error_code" in response:
        raise RuntimeError(f"FRED API error: {response.get('error_message')}")
    
    # Cache the observations if cache_dir is set
    if cache_dir:
        storage.cache_page(
            cache_dir,
            f"fred_obs_{series_id}.json",
            response,
        )
    
    return response.get("observations", [])
