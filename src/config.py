import json
from typing import Any, Dict


REQUIRED_KEYS = {"source", "output", "format"}


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    missing = REQUIRED_KEYS - set(config.keys())
    if missing:
        raise ValueError(f"Missing required config keys: {', '.join(sorted(missing))}")
    return config
