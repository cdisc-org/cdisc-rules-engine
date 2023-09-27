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
    "data, dataset_type, expected",
    [
        ({"values": [11, 12, 12, 5, 18, 9]}, PandasDataset, 5),
        ({"values": [11, 12, 12, 5, 18, 9]}, DaskDataset, 5),
    ],
)
def test_minimum(data, dataset_type, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    dataset = dataset_type.from_dict(data)
    operation_params.dataframe = dataset
    operation_params.target = "values"
    result = Minimum(operation_params, dataset, cache, data_service).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected
