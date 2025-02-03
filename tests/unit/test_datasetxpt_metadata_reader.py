"""
This module contains unit tests for DatasetMetadataReader class.
"""

import os
from unittest.mock import patch
from cdisc_rules_engine.services.datasetxpt_metadata_reader import (
    DatasetXPTMetadataReader,
)


def test_read_metadata():
    """
    Unit test for function read.
    Loads test .xpt file and extracts metadata.
    """
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"
    )
    reader = DatasetXPTMetadataReader(test_dataset_path, file_name="test_dataset.xpt")
    metadata: dict = reader.read()
    assert (
        metadata["dataset_name"] == "TEST_DATASET"
    ), "Wrong dataset name. Test file has been changed"
    assert (
        metadata["first_record"]["DOMAIN"] == "EX"
    ), "Wrong domain name. Test file has been changed"
    assert (
        metadata["dataset_label"] == "Exposure"
    ), "Wrong dataset label. Test file has been changed"
    assert (
        metadata["number_of_variables"] == 17
    ), "Wrong number of variables. Test file has been changed"
    assert (
        metadata["dataset_modification_date"] == "2020-08-21T09:14:26"
    ), "Incorrect modification date. Test file has been changed"
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


def test_read_metadata_with_estimate():
    """
    Unit test for metadata read using estimate length for xpt file.
    """
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"
    )
    with patch.dict(os.environ, {"DATASET_SIZE_THRESHOLD": "1"}):
        reader = DatasetXPTMetadataReader(
            test_dataset_path, file_name="test_dataset.xpt"
        )
        assert reader._estimate_dataset_length is True
        assert reader.row_limit == 1

        metadata: dict = reader.read()
        assert (
            metadata["dataset_name"] == "TEST_DATASET"
        ), "Wrong dataset name. Test file has been changed"
        assert (
            metadata["first_record"]["DOMAIN"] == "EX"
        ), "Wrong domain name. Test file has been changed"
        assert (
            metadata["dataset_label"] == "Exposure"
        ), "Wrong dataset label. Test file has been changed"
        assert (
            metadata["number_of_variables"] == 17
        ), "Wrong number of variables. Test file has been changed"
        assert (
            metadata["dataset_length"] == 1583
        ), "Wrong estimated dataset length. Test file or estimator has been changed"
        assert (
            metadata["dataset_modification_date"] == "2020-08-21T09:14:26"
        ), "Incorrect modification date. Test file has been changed"
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
    Loads test .xpt file and extracts metadata.
    """
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_adam_dataset.xpt"
    )
    reader = DatasetXPTMetadataReader(
        test_dataset_path, file_name="test_adam_dataset.xpt"
    )
    metadata: dict = reader.read()
    assert metadata["variable_formats"] == [
        "",
        "",
        "",
        "",
        "",
        "DATE9",
        "DATE9",
        "DATE9",
        "DATE9",
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
        "DATE9",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]


def test_read_header():
    """
    Unit test for the method _read_header.
    Loads test .xpt file and reads the header, returning the start of the dataset.
    Mock_open cannot simulate partial read() behavior so normal file reading is employed.
    """
    test_dataset_path = f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"

    reader = DatasetXPTMetadataReader(test_dataset_path, file_name="test_dataset.xpt")
    start_index = reader._read_header(test_dataset_path)

    assert start_index == 3120


def test_calculate_dataset_length_with_mocked_header():
    """
    Unit test for the method _calculate_dataset_length with mocked _read_header.
    Calculates the length of the dataset based on the  and row size.
    """
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"
    )
    reader = DatasetXPTMetadataReader(test_dataset_path, file_name="test_dataset.xpt")

    # Mock the _read_header method to return a start position
    with patch.object(reader, "_read_header", return_value=3120):
        expected_length = 1583
        dataset_length = reader._calculate_dataset_length()

    assert dataset_length == expected_length
