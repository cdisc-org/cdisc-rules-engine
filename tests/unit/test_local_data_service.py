import os
from unittest.mock import MagicMock

import pandas as pd
import pytest
from cdisc_rules_engine.config.config import ConfigService

from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.services.data_readers.data_reader_factory import (
    DataReaderFactory,
)


def test_read_metadata():
    """
    Unit test for read_data method.
    """
    dataset_path1 = f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"
    data_service1 = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    metadata1 = data_service1.read_metadata(dataset_path1)
    assert "file_metadata" in metadata1
    assert metadata1["file_metadata"].get("name") == "test_dataset.xpt"
    assert metadata1["file_metadata"].get("size") == 823120
    assert "contents_metadata" in metadata1
    assert "variable_labels" in metadata1["contents_metadata"]
    assert "variable_formats" in metadata1["contents_metadata"]
    assert "variable_name_to_label_map" in metadata1["contents_metadata"]
    assert "variable_name_to_size_map" in metadata1["contents_metadata"]
    assert "number_of_variables" in metadata1["contents_metadata"]
    assert "dataset_label" in metadata1["contents_metadata"]
    assert "domain_name" in metadata1["contents_metadata"]
    assert "dataset_modification_date" in metadata1["contents_metadata"]

    dataset_path2 = f"{os.path.dirname(__file__)}/../resources/test_dataset.json"
    data_service2 = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    metadata2 = data_service2.read_metadata(dataset_path2)
    assert "file_metadata" in metadata2
    assert metadata2["file_metadata"].get("name") == "test_dataset.json"
    assert metadata2["file_metadata"].get("size") == 101961
    assert "contents_metadata" in metadata2
    assert "variable_labels" in metadata2["contents_metadata"]
    assert "variable_formats" in metadata2["contents_metadata"]
    assert "variable_name_to_label_map" in metadata2["contents_metadata"]
    assert "variable_name_to_size_map" in metadata2["contents_metadata"]
    assert "number_of_variables" in metadata2["contents_metadata"]
    assert "dataset_label" in metadata2["contents_metadata"]
    assert "domain_name" in metadata2["contents_metadata"]
    assert "dataset_modification_date" in metadata2["contents_metadata"]


@pytest.mark.parametrize(
    "files, expected_result",
    [
        (["test_dataset.xpt", "test_defineV21-SDTM.xml"], True),
        (["test_dataset.json", "test_defineV21-SDTM.xml"], True),
        (["nope.xpt"], False),
    ],
)
def test_has_all_files(files, expected_result):
    directory = f"{os.path.dirname(__file__)}/../resources"
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    assert data_service.has_all_files(directory, files) == expected_result


def test_get_dataset():
    dataset_path1 = f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"
    mock_cache1 = MagicMock()
    mock_cache1.get.return_value = None
    data_service1 = LocalDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache1, data_format="xpt"
    )
    data1 = data_service1.get_dataset(dataset_name=dataset_path1)
    assert isinstance(data1, pd.DataFrame)

    dataset_path2 = f"{os.path.dirname(__file__)}/../resources/test_dataset.json"
    mock_cache2 = MagicMock()
    mock_cache2.get.return_value = None
    data_service2 = LocalDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache2, data_format="json"
    )
    data2 = data_service2.get_dataset(dataset_name=dataset_path2)
    assert isinstance(data2, pd.DataFrame)


def test_get_variables_metdata():
    dataset_path = f"{os.path.dirname(__file__)}/../resources/test_adam_dataset.xpt"
    mock_cache = MagicMock()
    mock_cache.get.return_value = None
    data_service = LocalDataService.get_instance(
        config=ConfigService(), cache_service=mock_cache, data_format="xpt"
    )
    data = data_service.get_variables_metadata(dataset_name=dataset_path)
    assert isinstance(data, pd.DataFrame)
    expected_keys = [
        "variable_name",
        "variable_format",
        "variable_order_number",
        "variable_data_type",
        "variable_label",
    ]
    for key in expected_keys:
        assert key in data
