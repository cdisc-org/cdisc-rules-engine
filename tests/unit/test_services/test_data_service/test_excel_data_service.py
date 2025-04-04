import os
from unittest.mock import MagicMock

import pytest
from cdisc_rules_engine.config.config import ConfigService

from cdisc_rules_engine.services.data_services import ExcelDataService
from cdisc_rules_engine.models.dataset import PandasDataset


@pytest.mark.parametrize(
    "dataset_name",
    ("ecaa.xpt", "ecbb.xpt", "suppec.xpt"),
)
def test_get_dataset(dataset_name):
    dataset_path = f"{os.path.dirname(__file__)}/../../../resources/test_datasets.xlsx"
    mock_cache = MagicMock()
    mock_cache.get_dataset.return_value = None
    data_service = ExcelDataService.get_instance(
        config=ConfigService(),
        cache_service=mock_cache,
        dataset_implementation=PandasDataset,
        dataset_path=dataset_path,
    )
    data = data_service.get_dataset(dataset_name=dataset_name)
    assert isinstance(data, PandasDataset)


@pytest.mark.parametrize(
    "expected_result",
    (
        {
            "dataset_location": "ecaa.xpt",
            "dataset_label": "Exposure as Collected AA",
            "dataset_name": "ECAA",
            "dataset_size": 0,
            "record_count": 8,
        },
        {
            "dataset_location": "ecbb.xpt",
            "dataset_label": "Exposure as Collected BB",
            "dataset_name": "ECBB",
            "dataset_size": 0,
            "record_count": 3,
        },
        {
            "dataset_location": "suppec.xpt",
            "dataset_label": "Supplemental Qualifiers for EC",
            "dataset_name": "SUPPEC",
            "dataset_size": 0,
            "record_count": 11,
        },
    ),
)
def test_get_dataset_metadata(expected_result):
    dataset_path = f"{os.path.dirname(__file__)}/../../../resources/test_datasets.xlsx"
    cache_mock = MagicMock()
    cache_mock.get_dataset.return_value = None
    data_service = ExcelDataService(
        cache_mock, MagicMock(), MagicMock(), dataset_path=dataset_path
    )
    metadata = data_service.get_dataset_metadata(
        dataset_name=expected_result["dataset_location"]
    )
    assert metadata["dataset_label"][0] == expected_result["dataset_label"]
    assert metadata["dataset_name"][0] == expected_result["dataset_name"]
    assert metadata["dataset_size"][0] == expected_result["dataset_size"]
    assert metadata["dataset_location"][0] == expected_result["dataset_location"]
    assert metadata["record_count"][0] == expected_result["record_count"]


@pytest.mark.parametrize(
    "dataset_name",
    ("ecaa.xpt", "ecbb.xpt", "suppec.xpt"),
)
def test_get_variables_metdata(dataset_name):
    dataset_path = f"{os.path.dirname(__file__)}/../../../resources/test_datasets.xlsx"
    mock_cache = MagicMock()
    mock_cache.get_dataset.return_value = None
    data_service = ExcelDataService.get_instance(
        config=ConfigService(),
        cache_service=mock_cache,
        dataset_implementation=PandasDataset,
        dataset_path=dataset_path,
    )
    data = data_service.get_variables_metadata(dataset_name=dataset_name, datasets=[])
    assert isinstance(data, PandasDataset)
    expected_keys = [
        "variable_name",
        "variable_format",
        "variable_order_number",
        "variable_data_type",
        "variable_label",
    ]
    for key in expected_keys:
        assert key in data
