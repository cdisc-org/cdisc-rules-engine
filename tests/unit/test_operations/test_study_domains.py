from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.study_domains import StudyDomains
from cdisc_rules_engine.models.operation_params import OperationParams

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
import pytest


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_get_study_domains_with_duplicates(
    operation_params: OperationParams, dataset_type
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    datasets = [
        {"filename": "dm.xpt", "domain": "DM"},
        {"filename": "dm1.xpt", "domain": "DM"},
        {"filename": "ae.xpt", "domain": "AE"},
        {"filename": "tv.xpt", "domain": "TV"},
    ]
    operation_params.datasets = datasets
    result = StudyDomains(
        operation_params, dataset_type.from_dict({"A": [1, 2, 3]}), cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert sorted(val) == ["AE", "DM", "TV"]


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_get_study_domains_with_missing_domains(
    operation_params: OperationParams, dataset_type
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    datasets = [
        {"filename": "dm.xpt"},
        {"filename": "dm1.xpt", "domain": "DM"},
        {"filename": "ae.xpt", "domain": "AE"},
        {"filename": "tv.xpt", "domain": "TV"},
    ]
    operation_params.datasets = datasets
    result = StudyDomains(
        operation_params, dataset_type.from_dict({"A": [1, 2, 3]}), cache, data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert sorted(val) == ["", "AE", "DM", "TV"]
