#!/usr/bin/env python
"""
Merge JSON schema files with their corresponding markdown descriptions.

This script:
1. Finds all .json schema files in the resources/schema directory
2. Looks for corresponding .md files with the same base name
3. Parses markdown sections (starting with ##)
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


def _save_section(
    level: int,
    name: str,
    description: List[str],
    level_descriptions: List[Dict[str, str]],
) -> None:
    """Save a section to the appropriate level dictionary."""
    idx = level - 1
    while len(level_descriptions) <= idx:
        level_descriptions.append({})
    level_descriptions[idx][name] = "\n".join(description)


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
    # Store dictionaries for each level (index 0 = level 1, index 1 = level 2, etc.)
    level_descriptions = []
    # Track current section for each level
    current_names = {}
    current_descriptions = {}

    for line in lines:
        if line.startswith("#"):
            # Count the number of # characters
            level = len(line) - len(line.lstrip("#"))
            if level >= 1:
                # Add this header line to all parent sections (those with level < current level)
                for parent_level in list(current_names.keys()):
                    if parent_level < level:
                        current_descriptions[parent_level].append(line)

                # Save all sections at this level or deeper that are currently open
                for existing_level in list(current_names.keys()):
                    if existing_level >= level:
                        _save_section(
                            existing_level,
                            current_names[existing_level],
                            current_descriptions[existing_level],
                            level_descriptions,
                        )
                        # Clear this level
                        del current_names[existing_level]
                        del current_descriptions[existing_level]

                # Start new section at this level
                current_names[level] = line.lstrip("#").strip()
                current_descriptions[level] = []
        else:
            # Add line to all currently open sections
            for level in current_names:
                current_descriptions[level].append(line)

    # Save any remaining open sections
    for level, name in current_names.items():
        _save_section(level, name, current_descriptions[level], level_descriptions)

    return level_descriptions


def attach_markdown_descriptions(obj: Any, descriptions: List[Dict[str, str]]) -> None:
    """
    Recursively traverse a JSON object and add markdownDescription properties.

    When a 'const' key is found with a value that matches a description key,
    adds 'markdownDescription' to the parent object. Searches through all
    header levels starting from the highest level (##).

    Args:
        obj: JSON object (dict, list, or primitive)
        descriptions: List of dictionaries of markdown descriptions, one per level
    """
    if isinstance(obj, dict):
        # Check if this object has a 'const' key
        if "const" in obj:
            # Search through all levels to find a matching description
            for level_dict in descriptions:
                if obj["const"] in level_dict:
                    obj["markdownDescription"] = level_dict[obj["const"]]
                    break

        # Recursively process all values
        for value in obj.values():
            attach_markdown_descriptions(value, descriptions)

    elif isinstance(obj, list):
        # Recursively process all list items
        for item in obj:
            attach_markdown_descriptions(item, descriptions)


def merge_schema_with_markdown(
    schema_path: Path, markdown_path: Path
) -> Dict[str, Any]:
    """
    Merge a JSON schema file with its corresponding markdown file.

    Args:
        schema_path: Path to the JSON schema file
        markdown_path: Path to the markdown file

    Returns:
        The merged JSON schema with markdownDescription properties added
    """
    # Load JSON schema
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Load and parse markdown if it exists
    if markdown_path.exists():
        with open(markdown_path, "r", encoding="utf-8") as f:
            md_content = f.read()

        descriptions = parse_markdown_to_dict(md_content)
        attach_markdown_descriptions(schema, descriptions)

    return schema


def main():
    """Main function to merge all schema files with their markdown descriptions."""
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Find all JSON schema files
    schema_files = list(SCHEMA_DIR.glob("*.json"))

    print(f"Processing {len(schema_files)} schema files...")

    for schema_path in schema_files:
        # Get corresponding markdown file
        md_path = schema_path.with_suffix(".md")

        # Merge schema with markdown
        merged_schema = merge_schema_with_markdown(schema_path, md_path)

        # Write to output directory
        output_path = OUTPUT_DIR / schema_path.name
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(merged_schema, f, indent=2)
            f.write("\n")

        print(f"✓ Processed {schema_path.name}")

    print(f"\nMerged schemas written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
