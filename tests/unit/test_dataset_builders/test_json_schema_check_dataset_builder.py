from unittest.mock import MagicMock
import pandas as pd

from cdisc_rules_engine.dataset_builders.json_schema_check_dataset_builder import (
    JsonSchemaCheckDatasetBuilder,
)
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset

# This test suite validates JsonSchemaCheckDatasetBuilder which returns a dataset
# where each JSON Schema validation error is a separate row.


def _make_builder(schema, instance):
    # Mock data_service exposing .json and dataset_implementation
    data_service = MagicMock()
    data_service.json = instance
    data_service.dataset_implementation = PandasDataset
    # Unused heavy dependencies are mocked.
    builder = JsonSchemaCheckDatasetBuilder(
        rule={},
        data_service=data_service,
        cache_service=MagicMock(),
        rule_processor=MagicMock(),
        data_processor=MagicMock(),
        datasets=[],
        dataset_metadata=MagicMock(),
        define_xml_path=None,
        standard="USDM",
        standard_version="4.0",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(standard_schema_definition=schema),
    )
    builder.get_parent_path = lambda path_list: (
        list(path_list)[:-1] if path_list else []
    )
    builder.parse_error = lambda error, errlist, errctx: (
        errlist.update(
            {
                "json_path": errlist.get("json_path", []) + [""],
                "error_context": errlist.get("error_context", []) + [""],
                "error_attribute": errlist.get("error_attribute", []) + [""],
                "error_value": errlist.get("error_value", []) + [""],
                "validator": errlist.get("validator", []) + [""],
                "validator_value": errlist.get("validator_value", []) + [""],
                "message": errlist.get("message", []) + [error.message],
                "instanceType": errlist.get("instanceType", []) + [""],
                "id": errlist.get("id", []) + [""],
                "_path": errlist.get("_path", []) + [""],
            }
        )
        if error.message not in errlist.get("message", [])
        else None
    )
    return builder


def test_json_schema_check_dataset_builder_valid():
    """
    Test that the builder correctly validates a valid JSON instance.
    """
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
        },
        "required": ["id", "name"],
    }
    instance = {"id": 1, "name": "Test"}

    data_service = MagicMock()
    data_service.json = instance
    data_service.dataset_implementation = PandasDataset
    data_service.dataset_path = "dummy.json"

    cache_service = MagicMock()
    # Ensure cache returns None to simulate empty cache
    cache_service.get.return_value = None

    builder = JsonSchemaCheckDatasetBuilder(
        rule={},
        data_service=data_service,
        cache_service=cache_service,
        rule_processor=MagicMock(),
        data_processor=MagicMock(),
        dataset_metadata=MagicMock(name="test_dataset"),
        define_xml_path=None,
        standard="USDM",
        standard_version="4.0",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(standard_schema_definition=schema),
    )

    dataset = builder.get_dataset()

    # Now expect a single row with all columns as empty strings or NaN,
    # except source_row_number
    rows = dataset.data.to_dict(orient="records")
    assert len(rows) == 1
    for key, value in rows[0].items():
        if key == "source_row_number":
            assert value == 1
        else:
            assert value == "" or pd.isna(value)


def test_json_schema_check_dataset_builder_invalid():
    """
    Test that the builder correctly identifies validation errors in an invalid JSON instance.
    """
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "items": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["id", "name"],
    }
    # invalid: id wrong type, name missing, items wrong types
    instance = {"id": "1", "items": [1, 2]}

    data_service = MagicMock()
    data_service.json = instance
    data_service.dataset_implementation = PandasDataset
    data_service.dataset_path = "dummy.xpt"

    cache_service = MagicMock()
    # Ensure cache returns None to simulate empty cache
    cache_service.get.return_value = None

    dataset_metadata = MagicMock()
    dataset_metadata.name = ""

    builder = JsonSchemaCheckDatasetBuilder(
        rule={},
        data_service=data_service,
        cache_service=cache_service,
        rule_processor=MagicMock(),
        data_processor=MagicMock(),
        dataset_metadata=dataset_metadata,
        define_xml_path=None,
        standard="USDM",
        standard_version="4.0",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(standard_schema_definition=schema),
    )

    # Retrieve the dataset
    ds = builder.get_dataset()
    assert not ds.empty

    # Collect rows as a list of dictionaries for convenience
    rows = ds.data.to_dict(orient="records")

    # Expect 4 specific errors (id type, required name, items element types)
    assert len(rows) == 4

    # Validate specific error messages
    expected_errors = [
        "'name' is a required property",
        "'1' is not of type 'integer'",
        "1 is not of type 'string'",
        "2 is not of type 'string'",
    ]
    actual_errors = [row["message"] for row in rows]
    for error in expected_errors:
        assert error in actual_errors

    # Verify that the cache_service.add method was called
    cache_service.add.assert_called_once()


def test_json_schema_missing_instance_type_is_reported():
    """
    When an entity in a discriminated anyOf union is missing its instanceType,
    the error must be captured and reported rather than silently dropped
    (regression for GitHub issue #1546).

    The dataset assignment fallback chain is:
      1. instanceType from data  →  2. schema title/ref  →  3. nearest ancestor
    DDF00125 checks for validator == "required" | "additionalProperties", so the
    inner `required` context error (not the top-level anyOf) must be surfaced with
    a meaningful dataset assignment.
    """
    schema = {
        "type": "object",
        "properties": {
            "study": {"$ref": "#/$defs/Study"},
        },
        "$defs": {
            "Study": {
                "title": "Study",
                "type": "object",
                "properties": {
                    "instanceType": {"const": "Study"},
                    "id": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {
                            "anyOf": [
                                {"$ref": "#/$defs/TypeA"},
                                {"$ref": "#/$defs/TypeB"},
                            ]
                        },
                    },
                },
                "required": ["instanceType", "id"],
            },
            "TypeA": {
                # title is intentionally present so the dataset assignment falls
                # back to "TypeA" (schema title) rather than "Study" (ancestor).
                "title": "TypeA",
                "type": "object",
                "properties": {
                    "instanceType": {"const": "TypeA"},
                    "value": {"type": "string"},
                },
                "required": ["instanceType", "value"],
            },
            "TypeB": {
                "title": "TypeB",
                "type": "object",
                "properties": {
                    "instanceType": {"const": "TypeB"},
                    "data": {"type": "integer"},
                },
                "required": ["instanceType", "data"],
            },
        },
    }

    # instanceType is intentionally missing from the nested item
    instance = {
        "study": {
            "instanceType": "Study",
            "id": "study-1",
            "items": [
                {"value": "some-value"},  # missing instanceType
            ],
        }
    }

    data_service = MagicMock()
    data_service.json = instance
    data_service.dataset_implementation = PandasDataset
    data_service.dataset_path = "dummy.json"

    cache_service = MagicMock()
    cache_service.get.return_value = None

    # Filter to the entity that owns the problematic object.
    # Because TypeA.title == "TypeA", the dataset column will be set to "TypeA"
    # (schema title wins over ancestor "Study").  This is the correct value for
    # DDF00125 to report the issue under the right entity.
    dataset_metadata = MagicMock()
    dataset_metadata.name = "TypeA"

    builder = JsonSchemaCheckDatasetBuilder(
        rule={},
        data_service=data_service,
        cache_service=cache_service,
        rule_processor=MagicMock(),
        data_processor=MagicMock(),
        dataset_metadata=dataset_metadata,
        define_xml_path=None,
        standard="USDM",
        standard_version="4.0",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(standard_schema_definition=schema),
    )

    ds = builder.get_dataset()
    rows = ds.data.to_dict(orient="records")

    # 1. Error must not be silently dropped
    assert len(rows) >= 1, "Missing instanceType error must produce at least one row"

    # 2. dataset must be set to the schema class name ("TypeA"), not empty string.
    #    This ensures DDF00125 associates the error with the correct entity.
    assert any(row.get("dataset") == "TypeA" for row in rows), (
        "Error with missing instanceType must be associated with the schema class "
        "('TypeA' via schema title), not silently dropped with dataset=''"
    )

    # 3. The error must have validator == "required" so DDF00125's check condition
    #    (validator is_contained_by ['required', 'additionalProperties']) fires.
    assert any(
        row.get("validator") == "required" for row in rows
    ), "The surfaced error must have validator='required' so DDF00125 can match it"

    # 4. The path should identify where in the document the problem is
    assert any(
        "/study/items/0" in (row.get("_path") or "") for row in rows
    ), "Path should point to the entity missing instanceType"
