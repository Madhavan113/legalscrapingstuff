import json
import urllib.parse
import urllib.request
from typing import Dict, Generator, List, Optional

from src import storage


CKAN_BASE_URL = "https://catalog.data.gov"
PACKAGE_SEARCH_ENDPOINT = "/api/3/action/package_search"
DEFAULT_PAGE_SIZE = 100


def _build_search_url(query: str, start: int, rows: int) -> str:
    params = urllib.parse.urlencode({"q": query, "start": start, "rows": rows})
    return f"{CKAN_BASE_URL}{PACKAGE_SEARCH_ENDPOINT}?{params}"


def _fetch_json(url: str) -> Dict:
    with urllib.request.urlopen(url) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _iter_cached_pages(cache_dir: str) -> Generator[Dict, None, None]:
    pages: List[Dict] = storage.load_cached_pages(cache_dir)
    for page in pages:
        if not page.get("success"):
            raise RuntimeError(f"Cached CKAN page failed: {page}")
        yield page


def iter_datasets(
    query: str = "*:*",
    limit: Optional[int] = None,
    cache_dir: Optional[str] = None,
    offline: bool = False,
) -> Generator[Dict, None, None]:
    fetched = 0
    start = 0
    page_size = DEFAULT_PAGE_SIZE
    page_index = 0

    if offline:
        pages = _iter_cached_pages(cache_dir or "")
    else:
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
            url = _build_search_url(query, start=start, rows=page_size)
            response = _fetch_json(url)
            storage.cache_page(cache_dir, f"page_{page_index:04d}.json", response)

        if not response.get("success"):
            raise RuntimeError(f"CKAN API call failed: {response}")

        results = response.get("result", {}).get("results", [])
        if not results:
            break

        for dataset in results:
            yield dataset
            fetched += 1
            if limit is not None and fetched >= limit:
                return

        start += len(results)
        page_index += 1
