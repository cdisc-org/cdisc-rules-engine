from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.record_count import RecordCount
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import pytest
from unittest.mock import MagicMock

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.services.data_services.data_service_factory import (
    DataServiceFactory,
)
import numpy as np


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_record_count_operation(operation_params: OperationParams, dataset_type):
    """
    Unit test for RecordCount operation.
    Creates a dataframe and checks that
    the operation returns correct number of records.
    """
    operation_params.dataframe = dataset_type.from_dict(
        {
            "STUDYID": [
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
            ],
            "AESEQ": [
                1,
                2,
            ],
            "USUBJID": [
                "TEST1",
                "TEST1",
            ],
        }
    )
    operation = RecordCount(
        operation_params, operation_params.dataframe, MagicMock(), MagicMock()
    )
    result: PandasDataset = operation.execute()
    expected: pd.Series = pd.Series(
        [
            2,
            2,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)


@pytest.mark.parametrize(
    "data, expected, filter",
    [
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                    "operation_id": [1, 1],
                }
            ),
            {"STUDYID": "CDISC02"},
        ),
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                    "operation_id": [0, 0],
                }
            ),
            {"STUDYID": "CDISC03"},
        ),
    ],
)
def test_filtered_record_count(
    data, expected, filter, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.filter = filter
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [2, 2, 3],
                    "USUBJID": ["TEST1", "TEST1", "TEST1"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [2, 2, 3],
                    "USUBJID": ["TEST1", "TEST1", "TEST1"],
                    "operation_id": [1, 1, 1],
                }
            ),
        ),
    ],
)
def test_multi_filter_record_count(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.filter = {"STUDYID": "CDISC02", "EQ": 2}
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)


@pytest.mark.parametrize(
    "data, expected, grouping_aliases",
    [
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 2, 1],
                }
            ),
            None,
        ),
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 1, None],
                }
            ),
            ["STUDYID2"],
        ),
    ],
)
def test_grouped_record_count(
    data, expected, grouping_aliases, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.grouping = ["STUDYID"]
    operation_params.grouping_aliases = grouping_aliases
    result = RecordCount(operation_params, data, cache, data_service).execute()
    grouping_column = "".join(
        operation_params.grouping_aliases or operation_params.grouping
    )
    expected = expected.data.replace(np.nan, None)
    assert operation_params.operation_id in result
    assert grouping_column in result
    assert result.data.equals(expected)


@pytest.mark.parametrize(
    "data, expected, grouping_aliases",
    [
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 2, 1],
                }
            ),
            None,
        ),
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 1, None],
                }
            ),
            ["STUDYID2", "DOMAIN"],
        ),
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 1, None],
                }
            ),
            ["STUDYID2"],
        ),
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "STUDYID2": ["CDISC01", "CDISC02", "CDISC03"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 1, None],
                }
            ),
            ["STUDYID2", "DOMAIN", "EXTRACOL"],
        ),
    ],
)
def test_multi_group_record_count(
    data, expected, grouping_aliases, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.grouping = ["STUDYID", "DOMAIN"]
    operation_params.grouping_aliases = grouping_aliases
    record_count = RecordCount(operation_params, data, cache, data_service)
    result = record_count.execute()
    grouping_columns = record_count._get_grouping_columns()
    expected = expected.data.replace(np.nan, None)
    assert operation_params.operation_id in result
    for grouping_column in grouping_columns:
        assert grouping_column in result
    assert result.data.equals(expected)


@pytest.mark.parametrize(
    "data, expected, grouping_aliases",
    [
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 3],
                    "USUBJID": ["TEST2", "TEST1", "TEST2"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 3],
                    "USUBJID": ["TEST2", "TEST1", "TEST2"],
                    "operation_id": [1, 1, 1],
                }
            ),
            None,
        ),
        (
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE", "AE"],
                    "EQ": [1, 2, 3, 4],
                    "USUBJID": ["TEST2", "TEST1", "TEST2", "TEST3"],
                    "USUBJID2": ["TEST2", "TEST1", "TEST3", "TEST4"],
                }
            ),
            PandasDataset.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE", "AE"],
                    "EQ": [1, 2, 3, 4],
                    "USUBJID": ["TEST2", "TEST1", "TEST2", "TEST3"],
                    "USUBJID2": ["TEST2", "TEST1", "TEST3", "TEST4"],
                    "operation_id": [1, 1, 0, None],
                }
            ),
            ["USUBJID2"],
        ),
    ],
)
def test_filtered_grouped_record_count(
    data, expected, grouping_aliases, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.grouping = ["USUBJID"]
    operation_params.filter = {"STUDYID": "CDISC01"}
    operation_params.grouping_aliases = grouping_aliases
    result = RecordCount(operation_params, data, cache, data_service).execute()
    grouping_column = "".join(
        operation_params.grouping_aliases or operation_params.grouping
    )
    expected = expected.data.replace(np.nan, None)
    assert operation_params.operation_id in result
    assert grouping_column in result
    assert result.data.equals(expected)
