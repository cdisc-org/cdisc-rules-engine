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
