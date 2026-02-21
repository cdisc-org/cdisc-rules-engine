#!/usr/bin/env python
"""
Merge JSON schema files with their corresponding markdown descriptions.

This script:
1. Finds all .json schema files in the resources/schema directory
2. Looks for corresponding .md files with the same base name
3. Parses markdown sections (starting with #)
4. Finds 'const' values in JSON schemas that match section names
5. Adds 'markdownDescription' properties to matching schema items
6. Outputs merged schemas to a new directory
"""

import json
from pathlib import Path
from typing import Any, Dict, List

# Directory constants
REPO_ROOT = Path(__file__).parent.parent
SCHEMA_DIR = REPO_ROOT / "resources" / "schema" / "rule"
OUTPUT_DIR = REPO_ROOT / "resources" / "schema" / "rule-merged"


def _ensure_and_save(
    level_descriptions: List[Dict[str, str]], level: int, name: str, content: List[str]
) -> None:
    """Ensure level_descriptions has enough dicts and save the section."""
    idx = level - 1
    level_descriptions.extend([{} for _ in range(idx - len(level_descriptions) + 1)])
    level_descriptions[idx][name] = "\n".join(content)


def parse_markdown_to_dict(md_content: str) -> List[Dict[str, str]]:
    """
    Parse markdown content into multiple dictionaries, one per header level.

    Each dictionary maps section names to descriptions for that level.
    Sections at level N continue until the next section at level N or higher.

    Args:
        md_content: The markdown file content as a string

    Returns:
        List of dictionaries, indexed by (level - 1).
        E.g., result[0] contains # headers, result[1] contains ## headers, etc.
    """
    lines = md_content.split("\n")
    level_descriptions = []
    current_names = {}
    current_descriptions = {}

    for line in lines:
        if line.startswith("#"):
            level = len(line) - len(line.lstrip("#"))
            for existing_level, name in list(current_names.items()):
                if existing_level < level:
                    current_descriptions[existing_level].append(line)
                else:
                    _ensure_and_save(
                        level_descriptions,
                        existing_level,
                        name,
                        current_descriptions[existing_level],
                    )
                    del current_names[existing_level]
                    del current_descriptions[existing_level]

            current_names[level] = line.lstrip("#").strip()
            current_descriptions[level] = []
        else:
            for desc_list in current_descriptions.values():
                desc_list.append(line)

    for level, name in current_names.items():
        _ensure_and_save(level_descriptions, level, name, current_descriptions[level])

    return level_descriptions


def attach_markdown_descriptions(obj: Any, descriptions: List[Dict[str, str]]) -> None:
    """Recursively add markdownDescription properties to objects with 'const' keys."""
    if isinstance(obj, dict):
        if "const" in obj:
            for level_dict in descriptions:
                if obj["const"] in level_dict:
                    obj["markdownDescription"] = level_dict[obj["const"]]
                    break
        for value in obj.values():
            attach_markdown_descriptions(value, descriptions)
    elif isinstance(obj, list):
        for item in obj:
            attach_markdown_descriptions(item, descriptions)


def merge_schema_with_markdown(
    schema_path: Path, markdown_path: Path
) -> Dict[str, Any]:
    """Merge a JSON schema file with its corresponding markdown file."""
    with open(schema_path, encoding="utf-8") as f:
        schema = json.load(f)

    if markdown_path.exists():
        with open(markdown_path, encoding="utf-8") as f:
            attach_markdown_descriptions(schema, parse_markdown_to_dict(f.read()))

    return schema


def main():
    """Merge all schema files with their markdown descriptions."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    schema_files = list(SCHEMA_DIR.glob("*.json"))
    print(f"Processing {len(schema_files)} schema files...")

    for schema_path in schema_files:
        merged_schema = merge_schema_with_markdown(
            schema_path, schema_path.with_suffix(".md")
        )

        with open(OUTPUT_DIR / schema_path.name, "w", encoding="utf-8") as f:
            json.dump(merged_schema, f, indent=2)
            f.write("\n")

        print(f"✓ Processed {schema_path.name}")

    print(f"\nMerged schemas written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
