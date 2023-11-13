"""
This module contains unit tests for DatasetJSONMetadataReader class.
"""
import os

from cdisc_rules_engine.services.datasetjson_metadata_reader import (
    DatasetJSONMetadataReader,
)


def test_read_metadata():
    """
    Unit test for function read.
    Loads test .json file and extracts metadata.
    """
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.json"
    )

    reader = DatasetJSONMetadataReader(test_dataset_path, file_name="test_dataset.json")
    metadata: dict = reader.read()

    assert metadata["dataset_name"] == "EX", "Test file has been changed"
    assert metadata["domain_name"] == "EX", "Test file has been changed"
    assert metadata["dataset_label"] == "Exposure", "Test file has been changed"
    assert metadata["number_of_variables"] == 18, "Test file has been changed"
    assert (
        metadata["dataset_modification_date"] == "2023-07-31T14:44:09"
    ), "Test file has been changed"
    assert isinstance(metadata["variable_labels"], list)
    assert isinstance(metadata["variable_names"], list)
    assert isinstance(metadata["variable_name_to_data_type_map"], dict)
    assert any(
        val in ["Char", "Num"]
        for val in metadata["variable_name_to_data_type_map"].values()
    )
    assert any(
        val not in ["string", "double", "Character", "Numeric"]
        for val in metadata["variable_name_to_data_type_map"].values()
    ), "pyreadstat values has not been converted"
    assert isinstance(metadata["variable_name_to_label_map"], dict)
    assert isinstance(metadata["variable_name_to_size_map"], dict)


def test_read_metadata_with_variable_formats():
    """
    Unit test for function read.
    Loads test .json file and extracts metadata.
    """
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.json"
    )

    reader = DatasetJSONMetadataReader(test_dataset_path, file_name="test_dataset.json")
    metadata: dict = reader.read()

    assert metadata["variable_formats"] == [
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]
