from unittest.mock import MagicMock
import pytest

from engine.services.local_data_service import LocalDataService
import os
import pandas as pd


def test_read_metadata():
    """
    Unit test for read_data method.
    """
    dataset_path = f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"
    data_service = LocalDataService()
    metadata = data_service.read_metadata(dataset_path)
    assert "file_metadata" in metadata
    assert metadata["file_metadata"].get("name") == "test_dataset.xpt"
    assert metadata["file_metadata"].get("size") == 823120
    assert "contents_metadata" in metadata
    assert "variable_labels" in metadata["contents_metadata"]
    assert "variable_name_to_label_map" in metadata["contents_metadata"]
    assert "variable_name_to_size_map" in metadata["contents_metadata"]
    assert "number_of_variables" in metadata["contents_metadata"]
    assert "dataset_label" in metadata["contents_metadata"]
    assert "domain_name" in metadata["contents_metadata"]
    assert "dataset_modification_date" in metadata["contents_metadata"]


@pytest.mark.parametrize(
    "files, expected_result",
    [
        (["test_dataset.xpt", "test_defineV21-SDTM.xml"], True),
        (["nope.xpt"], False),
    ],
)
def test_has_all_files(files, expected_result):
    directory = f"{os.path.dirname(__file__)}/../resources"
    data_service = LocalDataService()
    assert data_service.has_all_files(directory, files) == expected_result


def test_get_dataset():
    dataset_path = f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"
    mock_cache = MagicMock()
    mock_cache.get.return_value = None
    data_service = LocalDataService.get_instance(cache_service=mock_cache)
    data = data_service.get_dataset(dataset_name=dataset_path)
    assert isinstance(data, pd.DataFrame)
