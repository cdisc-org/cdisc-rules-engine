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
from typing import Any, Dict


def parse_markdown_to_dict(md_content: str) -> Dict[str, str]:
    """
    Parse markdown content into a dictionary mapping section names to descriptions.

    Sections start with '## ' or '### ' and content continues until the next section or end of file.
    Lines starting with '# ' (single hash) are ignored.

    Args:
        md_content: The markdown file content as a string

    Returns:
        Dictionary mapping section names to their markdown content
    """
    lines = md_content.split("\n")
    descriptions = {}
    current_name = None
    current_description = []

    for line in lines:
        # Check for section headers (## or ###)
        if line.startswith("### "):
            # Save previous section if it exists
            if current_name:
                descriptions[current_name] = "\n".join(current_description)
            # Start new section - remove the ### prefix
            current_name = line[4:].strip()
            current_description = []
        elif line.startswith("## "):
            # Save previous section if it exists
            if current_name:
                descriptions[current_name] = "\n".join(current_description)
            # Start new section - remove the ## prefix
            current_name = line[3:].strip()
            current_description = []
        elif not line.startswith("# "):
            # Add line to current section (skip lines starting with single #)
            current_description.append(line)

    # Save last section
    if current_name:
        descriptions[current_name] = "\n".join(current_description)

    return descriptions


def attach_markdown_descriptions(obj: Any, descriptions: Dict[str, str]) -> None:
    """
    Recursively traverse a JSON object and add markdownDescription properties.

    When a 'const' key is found with a value that matches a description key,
    adds 'markdownDescription' to the parent object.

    Args:
        obj: JSON object (dict, list, or primitive)
        descriptions: Dictionary of markdown descriptions keyed by const value
    """
    if isinstance(obj, dict):
        # Check if this object has a 'const' key
        if "const" in obj and obj["const"] in descriptions:
            obj["markdownDescription"] = descriptions[obj["const"]]

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
    # Define paths
    repo_root = Path(__file__).parent.parent
    schema_dir = repo_root / "resources" / "schema" / "rule"
    output_dir = repo_root / "resources" / "schema" / "rule-merged"

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all JSON schema files
    schema_files = list(schema_dir.glob("*.json"))

    print(f"Processing {len(schema_files)} schema files...")

    for schema_path in schema_files:
        # Get corresponding markdown file
        md_path = schema_path.with_suffix(".md")

        # Merge schema with markdown
        merged_schema = merge_schema_with_markdown(schema_path, md_path)

        # Write to output directory
        output_path = output_dir / schema_path.name
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(merged_schema, f, indent=2, ensure_ascii=False)

        print(f"✓ Processed {schema_path.name}")

    print(f"\nMerged schemas written to: {output_dir}")


if __name__ == "__main__":
    main()
