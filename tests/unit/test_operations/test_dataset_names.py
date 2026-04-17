from unittest.mock import MagicMock

from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.dataset_names import DatasetNames
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
import pytest


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_get_study_domains_with_duplicates(
    operation_params: OperationParams, dataset_type
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    datasets = [
        SDTMDatasetMetadata(**dataset)
        for dataset in [
            {"name": "DM", "filename": "dm.xpt", "first_record": {"DOMAIN": "DM"}},
            {"name": "DM1", "filename": "dm1.xpt", "first_record": {"DOMAIN": "DM"}},
            {"name": "AE", "filename": "ae.xpt", "first_record": {"DOMAIN": "AE"}},
            {"name": "TV", "filename": "tv.xpt", "first_record": {"DOMAIN": "TV"}},
        ]
    ]
    mock_data_service = MagicMock()
    mock_data_service.get_datasets.return_value = datasets
    result = DatasetNames(
        operation_params,
        dataset_type.from_dict({"A": [1, 2, 3]}),
        cache,
        mock_data_service,
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert sorted(val) == ["AE", "DM", "DM1", "TV"]


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_get_study_domains_with_missing_domains(
    operation_params: OperationParams, dataset_type
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    datasets = [
        SDTMDatasetMetadata(**dataset)
        for dataset in [
            {"first_record": {"DOMAIN": "TS"}},
            {"name": "DM1", "filename": "dm1.xpt", "first_record": {"DOMAIN": "DM"}},
            {"name": "AE", "filename": "AE.xpt", "first_record": {"DOMAIN": "AE"}},
            {"name": "TV", "filename": "tv.xpt", "first_record": {"DOMAIN": "TV"}},
        ]
    ]
    mock_data_service = MagicMock()
    mock_data_service.get_datasets.return_value = datasets
    result = DatasetNames(
        operation_params,
        dataset_type.from_dict({"A": [1, 2, 3]}),
        cache,
        mock_data_service,
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert sorted(val) == ["", "AE", "DM1", "TV"]
