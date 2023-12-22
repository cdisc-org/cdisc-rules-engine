import pandas as pd
import dask.dataframe as dd

import pytest
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.operation_params import OperationParams

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict({"values": [11, 12, 12, 5, 18, 9]}),
            {5, 9, 11, 12, 18},
        ),
        (
            dd.DataFrame.from_dict({"values": [11, 12, 12, 5, 18, 9]}, npartitions=1),
            {5, 9, 11, 12, 18},
        ),
    ],
)
def test_distinct(data, expected, operation_params: OperationParams):
    operation = DatasetOperations()
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    result = operation.get_service(
        name="distinct",
        operation_params=operation_params,
        original_dataset=data,
        cache=cache,
        data_service=data_service,
    )
    assert operation_params.operation_id in result
    assert len(result[operation_params.operation_id]) > 0
    # print("type of result: ", type(result[operation_params.operation_id]))
    # print(result)
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
        (
            dd.DataFrame.from_dict(
                {"values": [11, 12, 12, 5, 18, 9], "patient": [1, 2, 2, 1, 2, 1]},
                npartitions=1,
            ),
            {1: {5, 9, 11}, 2: {12, 18}},
        ),
    ],
)
def test_grouped_distinct(data, expected, operation_params: OperationParams):
    operation = DatasetOperations()
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    operation_params.grouping = ["patient"]
    result = operation.get_service(
        name="distinct",
        operation_params=operation_params,
        original_dataset=data,
        cache=cache,
        data_service=data_service,
    )
    assert operation_params.operation_id in result
    print("result : ", result)
    for _, val in result.iterrows():
        output = val[operation_params.operation_id]
        assert output == expected.get(val["patient"])
