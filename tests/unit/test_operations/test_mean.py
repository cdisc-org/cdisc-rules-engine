from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.mean import Mean
from cdisc_rules_engine.models.operation_params import OperationParams
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset


@pytest.mark.parametrize(
    "data, dataset_type, expected",
    [
        (
            {"values": [11, 12, 12, 5, 18, 9]},
            PandasDataset,
            11.166666666666666,
        ),
        (
            {"values": [11, 12, 12, 5, 18, 9]},
            DaskDataset,
            11.166666666666666,
        ),
    ],
)
def test_mean(data, dataset_type, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    dataset = dataset_type.from_dict(data)
    operation_params.dataframe = dataset
    operation_params.target = "values"
    result = Mean(operation_params, dataset, cache, data_service).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected


@pytest.mark.parametrize(
    "data, dataset_type, expected",
    [
        (
            {"values": [11, 12, 12, 5, 18, 9], "patient": [1, 2, 2, 1, 2, 1]},
            PandasDataset,
            {1: 8.333333333333334, 2: 14.0},
        ),
        (
            {"values": [11, 12, 12, 5, 18, 9], "patient": [1, 2, 2, 1, 2, 1]},
            DaskDataset,
            {1: 8.333333333333334, 2: 14.0},
        ),
    ],
)
def test_grouped_mean(data, dataset_type, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    dataset = dataset_type.from_dict(data)
    operation_params.dataframe = dataset
    operation_params.target = "values"
    operation_params.grouping = ["patient"]
    result = Mean(operation_params, dataset, cache, data_service).execute()
    assert operation_params.operation_id in result
    for _, val in result.iterrows():
        assert val[operation_params.operation_id] == expected.get(val["patient"])
