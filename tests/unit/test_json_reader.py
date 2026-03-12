import os
import pytest
from cdisc_rules_engine.services.data_readers.json_reader import JSONReader
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidJSONFormat


def test_json_reader_returns_dict():
    test_dataset_path = (
        f"{os.path.dirname(__file__)}/../resources/Datasets_whitespace.json"
    )
    json_data = JSONReader(encoding="utf-8").from_file(test_dataset_path)
    assert isinstance(json_data, dict)
    assert "datasets" in json_data


def test_whitespace_from_record_keys():
    test_dataset_path = (
        f"{os.path.dirname(__file__)}/../resources/Datasets_whitespace.json"
    )
    with pytest.raises(InvalidJSONFormat) as exc_info:
        JSONReader(encoding="utf-8").from_file(test_dataset_path)
    assert "leading/trailing whitespace" in str(exc_info.value.message)
