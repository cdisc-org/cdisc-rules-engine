from unittest.mock import MagicMock

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
        dataset_path="dummy.xpt",
        datasets=[],
        dataset_metadata=MagicMock(),
        define_xml_path=None,
        standard="USDM",
        standard_version="4.0",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(standard_schema_definition=schema),
    )
    return builder


def test_json_schema_check_dataset_builder_valid():
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "items": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["id", "name"],
    }
    instance = {"id": 1, "name": "abc", "items": ["x", "y"]}
    builder = _make_builder(schema, instance)
    ds = builder.get_dataset()
    # Expect empty dataset with defined columns.
    expected_cols = [
        "path",
        "message",
        "validator",
        "validator_value",
        "schema_path",
    ]
    assert list(ds.columns) == expected_cols
    assert ds.empty


def test_json_schema_check_dataset_builder_invalid():
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
    builder = _make_builder(schema, instance)
    ds = builder.get_dataset()
    assert not ds.empty
    # Collect rows as list of dicts for convenience.
    rows = ds.data.to_dict(orient="records")
    # Expect >=3 errors (id type, required name, items element types)
    assert len(rows) >= 3
    paths = {r["path"] for r in rows}
    validators = {r["validator"] for r in rows}
    messages = {r["message"] for r in rows}
    assert "required" in validators
    # id type error
    assert "id" in paths
    # at least one items index type error
    assert any(p.startswith("items.") for p in paths)
    assert "type" in validators
    # Check specific messages
    assert "'name' is a required property" in messages
    # id wrong type message (could vary, so partial substring check)
    assert any("is not of type" in m or "is not" in m for m in messages)
    # Ensure schema_path column populated
    assert all(r["schema_path"] for r in rows)
