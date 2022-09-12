from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.mean import Mean
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict({"values": [11, 12, 12, 5, 18, 9]}),
            11.166666666666666,
        ),
    ],
)
def test_mean(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    result = Mean(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {"values": [11, 12, 12, 5, 18, 9], "patient": [1, 2, 2, 1, 2, 1]}
            ),
            {1: 8.333333333333334, 2: 14.0},
        ),
    ],
)
def test_grouped_mean(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    operation_params.grouping = ["patient"]
    result = Mean(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    for _, val in result.iterrows():
        assert val[operation_params.operation_id] == expected.get(val["patient"])
