import os
from cdisc_rules_engine.services.data_readers.json_reader import JSONReader


def test_json_reader_returns_dict():
    test_dataset_path = (
        f"{os.path.dirname(__file__)}/../resources/Datasets_whitespace.json"
    )
    json_data = JSONReader(encoding="utf-8").from_file(test_dataset_path)
    assert isinstance(json_data, dict)
    assert "datasets" in json_data


def test_whitespace_stripped_from_record_keys():
    test_dataset_path = (
        f"{os.path.dirname(__file__)}/../resources/Datasets_whitespace.json"
    )
    json_data = JSONReader(encoding="utf-8").from_file(test_dataset_path)
    for dataset in json_data.get("datasets", []):
        for key in dataset.get("records", {}).keys():
            assert key == key.strip(), f"Key '{key}' has leading/trailing whitespace"
