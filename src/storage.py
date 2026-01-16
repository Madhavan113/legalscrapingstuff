import json
import os
from typing import Dict, List, Optional


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def cache_page(cache_dir: Optional[str], filename: str, payload: Dict) -> None:
    if not cache_dir:
        return
    ensure_dir(cache_dir)
    path = os.path.join(cache_dir, filename)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def load_cached_pages(cache_dir: str) -> List[Dict]:
    if not os.path.isdir(cache_dir):
        raise FileNotFoundError(f"Cache directory not found: {cache_dir}")

    files = sorted(
        [name for name in os.listdir(cache_dir) if name.endswith(".json")]
    )
    if not files:
        raise FileNotFoundError(f"No cached pages found in {cache_dir}")

    pages: List[Dict] = []
    for name in files:
        path = os.path.join(cache_dir, name)
        with open(path, "r", encoding="utf-8") as handle:
            pages.append(json.load(handle))
    return pages
