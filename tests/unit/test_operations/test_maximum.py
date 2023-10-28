from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import dask.dataframe as dd
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


@pytest.mark.parametrize(
    "data, expected",
    [
        (pd.DataFrame.from_dict({"values": [11, 12, 12, 5, 18, 9]}), 18),
        (pd.DataFrame.from_dict({"values": [11, 12, 12, 5, 18, 29]}), 29),
        (dd.DataFrame.from_dict({"values": [11, 12, 12, 5, 18, 9]}, npartitions=1), 18),
        (
            dd.DataFrame.from_dict({"values": [11, 12, 12, 5, 18, 29]}, npartitions=1),
            29,
        ),
    ],
)
def test_minimum(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    operations = DatasetOperations()
    result = operations.get_service("max", operation_params, data, cache, data_service)
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected
