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
    """Test parsing markdown content into a dictionary."""
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

    assert "Section One" in result
    assert "Subsection" in result
    assert "Section Two" in result
    assert "This is section one content." in result["Section One"]
    assert "This is a subsection." in result["Subsection"]


def test_parse_markdown_ignores_single_hash():
    """Test that markdown parser ignores lines starting with single #."""
    markdown = """# This should be ignored
## Valid Section
This content should be included.
# This line should be ignored too
More valid content.
"""
    result = parse_markdown_to_dict(markdown)

    assert "Valid Section" in result
    assert "# This should be ignored" not in result["Valid Section"]
    assert "# This line should be ignored too" not in result["Valid Section"]
    assert "This content should be included." in result["Valid Section"]


def test_attach_markdown_descriptions():
    """Test adding markdownDescription to schema objects."""
    schema = {
        "anyOf": [
            {"const": "Option One", "title": "First option"},
            {"const": "Option Two", "title": "Second option"},
        ]
    }
    descriptions = {
        "Option One": "Description for option one",
        "Option Two": "Description for option two",
    }

    attach_markdown_descriptions(schema, descriptions)

    assert schema["anyOf"][0]["markdownDescription"] == "Description for option one"
    assert schema["anyOf"][1]["markdownDescription"] == "Description for option two"


def test_attach_markdown_descriptions_nested():
    """Test adding markdownDescription to nested schema objects."""
    schema = {"anyOf": [{"properties": {"operator": {"const": "equal_to"}}}]}
    descriptions = {"equal_to": "Checks if values are equal"}

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

    assert "#anchor" in result
    assert "This is content for a section that starts with #." in result["#anchor"]
