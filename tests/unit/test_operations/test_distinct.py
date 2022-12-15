from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.distinct import Distinct
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
            {5, 9, 11, 12, 18},
        ),
    ],
)
def test_distinct(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    result = Distinct(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert len(result[operation_params.operation_id]) > 0
    for val in result[operation_params.operation_id]:
        assert val == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {"values": [11, 12, 12, 5, 18, 9], "patient": [1, 2, 2, 1, 2, 1]}
            ),
            {1: {5, 9, 11}, 2: {12, 18}},
        ),
    ],
)
def test_grouped_distinct(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    operation_params.grouping = ["patient"]
    result = Distinct(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    for _, val in result.iterrows():
        assert val[operation_params.operation_id] == expected.get(val["patient"])
