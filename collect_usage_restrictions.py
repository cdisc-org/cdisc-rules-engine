"""
collect_usage_restrictions.py

Walks the cached CDISC Library metadata (standards_details.pkl and
standards_models.pkl) and collects every distinct value found under any
"usageRestrictions" key, wherever it appears in the nested structure.

Run from the repo root:
    python collect_usage_restrictions.py
"""

import pickle
from collections import defaultdict
from pathlib import Path

CACHE_DIR = Path("resources/cache")


def walk_and_collect(node, path, results):
    """Recursively look for 'usageRestrictions' keys anywhere in the structure."""
    if isinstance(node, dict):
        if "usageRestrictions" in node:
            value = node["usageRestrictions"]
            name = node.get("name", "<unknown>")
            key = _normalize(value)
            results[key].append(f"{path} / {name}")
        for k, v in node.items():
            walk_and_collect(v, f"{path}.{k}", results)
    elif isinstance(node, list):
        for i, item in enumerate(node):
            walk_and_collect(item, f"{path}[{i}]", results)


def _normalize(value):
    # usageRestrictions could be a plain string or a list; normalize for grouping.
    if isinstance(value, list):
        return tuple(sorted(str(v) for v in value))
    return value


def load_pickle(filename):
    file_path = CACHE_DIR / filename
    if not file_path.exists():
        print(f"Skipping {file_path} (not found)")
        return {}
    with open(file_path, "rb") as f:
        return pickle.load(f)


def main():
    results = defaultdict(list)

    for filename in ("standards_details.pkl", "standards_models.pkl"):
        data = load_pickle(filename)
        for cache_key, entry in data.items():
            walk_and_collect(entry, cache_key, results)

    print(f"Found {len(results)} distinct usageRestrictions value(s):\n")
    for value, locations in results.items():
        print(repr(value))
        print(f"  seen {len(locations)} time(s), e.g.: {locations[0]}")
        print()


if __name__ == "__main__":
    main()