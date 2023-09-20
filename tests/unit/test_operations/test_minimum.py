from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.minimum import Minimum
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"values": [11, 12, 12, 5, 18, 9]}, 5),
    ],
)
def test_minimum(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    dataset = PandasDataset.from_dict(data)
    operation_params.dataframe = dataset
    operation_params.target = "values"
    result = Minimum(operation_params, dataset, cache, data_service).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"values": [11, 12, 12, 5, 18, 9]}, 5),
    ],
)
def test_minimum_dask(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    dataset = DaskDataset.from_dict(data)
    operation_params.dataframe = dataset
    operation_params.target = "values"
    result = Minimum(operation_params, dataset, cache, data_service).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected
