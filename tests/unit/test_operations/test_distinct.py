from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.distinct import Distinct
from cdisc_rules_engine.models.operation_params import OperationParams

import pytest
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            PandasDataset.from_dict({"values": [11, 12, 12, 5, 18, 9]}),
            {5, 9, 11, 12, 18},
        ),
        (
            DaskDataset.from_dict({"values": [11, 12, 12, 5, 18, 9]}),
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
    "data, expected, grouping_aliases",
    [
        (
            PandasDataset.from_dict(
                {"values": [11, 12, 12, 5, 18, 9], "patient": [1, 2, 2, 1, 2, 1]}
            ),
            {1: {5, 9, 11}, 2: {12, 18}},
            None,
        ),
        (
            DaskDataset.from_dict(
                {"values": [11, 12, 12, 5, 18, 9], "patient": [1, 2, 2, 1, 2, 1]}
            ),
            {1: {5, 9, 11}, 2: {12, 18}},
            None,
        ),
        (
            PandasDataset.from_dict(
                {
                    "values": [11, 12, 12, 5, 18, 9],
                    "patient": [1, 2, 2, 1, 2, 1],
                    "subject": [1, 2, 2, 1, 2, 3],
                }
            ),
            {1: {5, 9, 11}, 2: {12, 18}, 3: None},
            ["subject"],
        ),
        (
            DaskDataset.from_dict(
                {
                    "values": [11, 12, 12, 5, 18, 9],
                    "patient": [1, 2, 2, 1, 2, 1],
                    "subject": [1, 2, 2, 1, 2, 3],
                }
            ),
            {1: {5, 9, 11}, 2: {12, 18}, 3: None},
            ["subject"],
        ),
    ],
)
def test_grouped_distinct(
    data, expected, grouping_aliases, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    operation_params.grouping = ["patient"]
    operation_params.grouping_aliases = grouping_aliases
    result = Distinct(operation_params, data, cache, data_service).execute()
    grouping_column = "".join(
        operation_params.grouping_aliases or operation_params.grouping
    )
    assert operation_params.operation_id in result
    assert grouping_column in result
    for _, val in result.iterrows():
        assert val[operation_params.operation_id] == expected.get(val[grouping_column])


@pytest.mark.parametrize(
    "data, expected, grouping_aliases, filter",
    [
        (
            PandasDataset.from_dict(
                {
                    "values": [11, 12, 12, 5, 18, 9],
                    "patient": [1, 2, 2, 1, 2, 1],
                    "cat": [1, 1, 1, 1, 2, 1],
                    "scat": ["a", "a", "a", "a", "a", "b"],
                }
            ),
            {1: {5, 11}, 2: {12}},
            None,
            {"cat": 1, "scat": "a"},
        ),
        (
            DaskDataset.from_dict(
                {
                    "values": [11, 12, 12, 5, 18, 9],
                    "patient": [1, 2, 2, 1, 2, 1],
                    "cat": [1, 1, 1, 1, 2, 1],
                    "scat": ["a", "a", "a", "a", "a", "b"],
                }
            ),
            {1: {5, 11}, 2: {12}},
            None,
            {"cat": 1, "scat": "a"},
        ),
        (
            PandasDataset.from_dict(
                {
                    "values": [11, 12, 12, 5, 18, 9],
                    "patient": [1, 2, 2, 1, 2, 1],
                    "cat": [1, 1, 1, 1, 2, 1],
                    "scat": ["a", "a", "a", "a", "a", "b"],
                    "subject": [1, 2, 2, 1, 2, 3],
                }
            ),
            {1: {5, 11}, 2: {12}, 3: None},
            ["subject"],
            {"cat": 1, "scat": "a"},
        ),
        (
            DaskDataset.from_dict(
                {
                    "values": [11, 12, 12, 5, 18, 9],
                    "patient": [1, 2, 2, 1, 2, 1],
                    "cat": [1, 1, 1, 1, 2, 1],
                    "scat": ["a", "a", "a", "a", "a", "b"],
                    "subject": [1, 2, 2, 1, 2, 3],
                }
            ),
            {1: {5, 11}, 2: {12}, 3: None},
            ["subject"],
            {"cat": 1, "scat": "a"},
        ),
    ],
)
def test_filtered_grouped_distinct(
    data, expected, grouping_aliases, filter, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.target = "values"
    operation_params.filter = filter
    operation_params.grouping = ["patient"]
    operation_params.grouping_aliases = grouping_aliases
    result = Distinct(operation_params, data, cache, data_service).execute()
    grouping_column = "".join(
        operation_params.grouping_aliases or operation_params.grouping
    )
    assert operation_params.operation_id in result
    assert grouping_column in result
    for _, val in result.iterrows():
        assert val[operation_params.operation_id] == expected.get(val[grouping_column])
