"""
Tests for the schema markdown merge script.
"""

import json
import tempfile
from pathlib import Path

from scripts.merge_schema_markdown import (
    parse_markdown_to_dict,
    attach_markdown_descriptions,
    merge_schema_with_markdown,
)


def test_parse_markdown_to_dict():
    """Test parsing markdown content into multiple level dictionaries."""
    markdown = """# Title
## Section One
This is section one content.
More content here.

### Subsection
This is a subsection.

## Section Two
This is section two.
"""
    result = parse_markdown_to_dict(markdown)

    # Result is a list of dictionaries, one per level
    assert isinstance(result, list)
    assert len(result) >= 2  # At least # and ## levels

    # Level 0 contains # headers, Level 1 contains ## headers, Level 2 contains ### headers
    # Find dictionaries containing our sections
    all_sections = {}
    for level_dict in result:
        all_sections.update(level_dict)

    assert "Section One" in all_sections
    assert "Subsection" in all_sections
    assert "Section Two" in all_sections
    assert "This is section one content." in all_sections["Section One"]
    assert "This is a subsection." in all_sections["Subsection"]


def test_parse_markdown_includes_nested_headers():
    """Test that parent sections include nested header lines."""
    markdown = """## Parent Section
Parent content.

### Child Section
Child content.

#### Grandchild Section
Grandchild content.
"""
    result = parse_markdown_to_dict(markdown)

    # Find the parent section
    all_sections = {}
    for level_dict in result:
        all_sections.update(level_dict)

    parent = all_sections["Parent Section"]
    # Parent should include the ### and #### headers in its content
    assert "### Child Section" in parent
    assert "#### Grandchild Section" in parent
    assert "Child content." in parent
    assert "Grandchild content." in parent


def test_parse_markdown_processes_all_header_levels():
    """Test that markdown parser processes all header levels starting from #."""
    markdown = """# Top Level
Top content.

## Second Level
Second content.

### Third Level
Third content.
"""
    result = parse_markdown_to_dict(markdown)

    # Find all sections across all levels
    all_sections = {}
    for level_dict in result:
        all_sections.update(level_dict)

    assert "Top Level" in all_sections
    assert "Second Level" in all_sections
    assert "Third Level" in all_sections
    assert "Top content." in all_sections["Top Level"]


def test_attach_markdown_descriptions():
    """Test adding markdownDescription to schema objects."""
    schema = {
        "anyOf": [
            {"const": "Option One", "title": "First option"},
            {"const": "Option Two", "title": "Second option"},
        ]
    }
    # List of dictionaries, one per level
    descriptions = [
        {"Option One": "Description for option one"},
        {"Option Two": "Description for option two"},
    ]

    attach_markdown_descriptions(schema, descriptions)

    assert schema["anyOf"][0]["markdownDescription"] == "Description for option one"
    assert schema["anyOf"][1]["markdownDescription"] == "Description for option two"


def test_attach_markdown_descriptions_nested():
    """Test adding markdownDescription to nested schema objects."""
    schema = {"anyOf": [{"properties": {"operator": {"const": "equal_to"}}}]}
    descriptions = [{"equal_to": "Checks if values are equal"}]

    attach_markdown_descriptions(schema, descriptions)

    assert (
        schema["anyOf"][0]["properties"]["operator"]["markdownDescription"]
        == "Checks if values are equal"
    )


def test_merge_schema_with_markdown():
    """Test merging a schema file with markdown file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create test schema file
        schema = {
            "$id": "test.json",
            "anyOf": [{"const": "Test Value", "title": "A test"}],
        }
        schema_path = tmpdir / "test.json"
        with open(schema_path, "w") as f:
            json.dump(schema, f)

        # Create test markdown file (with blank line after heading like real files)
        markdown = """## Test Value

This is a test description.
"""
        md_path = tmpdir / "test.md"
        with open(md_path, "w") as f:
            f.write(markdown)

        # Merge
        result = merge_schema_with_markdown(schema_path, md_path)

        # Note: markdown content after ## heading includes empty line before content
        assert (
            result["anyOf"][0]["markdownDescription"]
            == "\nThis is a test description.\n"
        )


def test_merge_schema_without_markdown():
    """Test merging when markdown file doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create test schema file
        schema = {
            "$id": "test.json",
            "anyOf": [{"const": "Test Value", "title": "A test"}],
        }
        schema_path = tmpdir / "test.json"
        with open(schema_path, "w") as f:
            json.dump(schema, f)

        # Non-existent markdown file
        md_path = tmpdir / "test.md"

        # Merge should work without error
        result = merge_schema_with_markdown(schema_path, md_path)

        # Schema should be unchanged
        assert "markdownDescription" not in result["anyOf"][0]
        assert result["anyOf"][0]["const"] == "Test Value"


def test_section_name_with_hash():
    """Test that section names starting with # are preserved."""
    markdown = """## #anchor
This is content for a section that starts with #.
"""
    result = parse_markdown_to_dict(markdown)

    # Find the section across all levels
    all_sections = {}
    for level_dict in result:
        all_sections.update(level_dict)

    assert "#anchor" in all_sections
    assert (
        "This is content for a section that starts with #." in all_sections["#anchor"]
    )


def test_attach_markdown_descriptions_searches_all_levels():
    """Test that descriptions are found across multiple header levels."""
    schema = {
        "anyOf": [
            {"const": "Top Level Item", "title": "Top"},
            {"const": "Nested Item", "title": "Nested"},
        ]
    }
    # Descriptions at different levels
    descriptions = [
        {"Top Level Item": "Description from level 1"},
        {"Nested Item": "Description from level 2"},
    ]

    attach_markdown_descriptions(schema, descriptions)

    assert schema["anyOf"][0]["markdownDescription"] == "Description from level 1"
    assert schema["anyOf"][1]["markdownDescription"] == "Description from level 2"
