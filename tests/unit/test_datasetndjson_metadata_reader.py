"""
This module contains unit tests for DatasetNDJSONMetadataReader class.
"""

import os

from cdisc_rules_engine.services.datasetndjson_metadata_reader import (
    DatasetNDJSONMetadataReader,
)


def test_read_metadata():
    """
    Unit test for function read.
    Loads test .ndjson file and extracts metadata.
    """
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.ndjson"
    )

    reader = DatasetNDJSONMetadataReader(
        test_dataset_path, file_name="test_dataset.ndjson"
    )
    metadata: dict = reader.read()

    assert metadata["dataset_name"] == "EX", "Test file has been changed"
    assert metadata["first_record"]["DOMAIN"] == "EX", "Test file has been changed"
    assert metadata["dataset_label"] == "Exposure", "Test file has been changed"
    assert metadata["number_of_variables"] == 17, "Test file has been changed"
    assert (
        metadata["dataset_modification_date"] == "2024-11-11T15:09:16"
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
    Loads test .ndjson file and extracts metadata.
    """
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.ndjson"
    )

    reader = DatasetNDJSONMetadataReader(
        test_dataset_path, file_name="test_dataset.ndjson"
    )
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
    ]
