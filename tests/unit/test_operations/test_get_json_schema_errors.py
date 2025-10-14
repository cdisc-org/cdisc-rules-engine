import json
from unittest.mock import MagicMock

from cdisc_rules_engine.operations.get_json_schema_errors import GetJsonSchemaErrors
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)


def _build_params(dataset):
    return OperationParams(
        core_id="CORE-JSON-SCHEMA",
        dataframe=dataset.data,
        dataset_path="dummy.xpt",
        datasets=[],
        domain="DM",
        directory_path="/tmp",
        operation_id="get_json_schema_errors",
        operation_name="get_json_schema_errors",
        standard="SDTMIG",
        standard_version="3.3",
    )


def test_get_json_schema_errors_valid():
    # schema expecting integer id, string name, array of string items
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "items": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["id", "name"],
    }
    instance = {"id": 1, "name": "test", "items": ["a", "b"]}
    dataset = PandasDataset.from_dict({"dummy": [1]})
    params = _build_params(dataset)
    # minimal data_service exposing .json
    data_service = MagicMock()
    data_service.json = instance
    op = GetJsonSchemaErrors(
        params,
        dataset,
        cache_service=MagicMock(),
        data_service=data_service,
        library_metadata=LibraryMetadataContainer(standard_schema_definition=schema),
    )
    result_dataset = op.execute()
    errors_list = result_dataset[params.operation_id].iloc[0]
    assert isinstance(errors_list, list)
    assert errors_list == []  # no validation errors


def test_get_json_schema_errors_invalid():
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "items": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["id", "name"],
    }
    # invalid: id wrong type, name missing, items contain non-strings
    instance = {"id": "1", "items": [1, 2]}
    dataset = PandasDataset.from_dict({"dummy": [1]})
    params = _build_params(dataset)
    data_service = MagicMock()
    data_service.json = instance
    op = GetJsonSchemaErrors(
        params,
        dataset,
        cache_service=MagicMock(),
        data_service=data_service,
        library_metadata=LibraryMetadataContainer(standard_schema_definition=schema),
    )
    result_dataset = op.execute()
    errors_serialized = result_dataset[params.operation_id].iloc[0]
    assert isinstance(errors_serialized, list)
    assert len(errors_serialized) >= 3  # id type, missing name, items element types
    parsed = [json.loads(e) for e in errors_serialized]
    # collect helpers
    by_path = {e["path"]: e for e in parsed}
    # required property error at root
    assert any(e["validator"] == "required" for e in parsed)
    # id type error
    assert "id" in by_path and by_path["id"]["validator"] == "type"
    # at least one items.* type error
    assert any(
        p.startswith("items.") and e["validator"] == "type" for p, e in by_path.items()
    )
