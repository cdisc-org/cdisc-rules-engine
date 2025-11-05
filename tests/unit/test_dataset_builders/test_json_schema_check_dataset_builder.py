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
        dataset_path="dummy.xpt",
        datasets=[],
        dataset_metadata=MagicMock(name="test_dataset"),
        define_xml_path=None,
        standard="USDM",
        standard_version="4.0",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(standard_schema_definition=schema),
    )

    dataset = builder.get_dataset()

    assert dataset.empty


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
        dataset_path="dummy.xpt",
        datasets=[],
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
