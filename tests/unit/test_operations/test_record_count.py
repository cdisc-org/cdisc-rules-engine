from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.record_count import RecordCount
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import pytest


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
    result: pd.DataFrame = operation.execute()
    expected: pd.Series = pd.Series(
        [
            2,
            2,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)

@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                }
            ),
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE"],
                    "EQ": [1, 2],
                    "USUBJID": ["TEST1", "TEST1"],
                    "operation_id": [1, 1],
                }
            ),
        ),
    ],
)
def test_filtered_record_count(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.filter = {"STUDYID": "CDISC02"}
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 3],
                    "USUBJID": ["TEST1", "TEST1", "TEST1"],
                }
            ),
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC02", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 3],
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
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 2, 1],
                }
            ),
        ),
    ],
)
def test_grouped_record_count(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.grouping = ["STUDYID"]
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                }
            ),
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 2],
                    "USUBJID": ["TEST1", "TEST1", "TEST2"],
                    "operation_id": [2, 2, 1],
                }
            ),
        ),
    ],
)
def test_multi_group_record_count(data, expected, operation_params: OperationParams):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.grouping = ["STUDYID", "DOMAIN"]
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 3],
                    "USUBJID": ["TEST2", "TEST1", "TEST2"],
                }
            ),
            pd.DataFrame.from_dict(
                {
                    "STUDYID": ["CDISC01", "CDISC01", "CDISC02"],
                    "DOMAIN": ["AE", "AE", "AE"],
                    "EQ": [1, 2, 3],
                    "USUBJID": ["TEST2", "TEST1", "TEST2"],
                    "operation_id": [1, 1, 1],
                }
            ),
        ),
    ],
)
def test_filtered_grouped_record_count(
    data, expected, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    data_service = DataServiceFactory(config, cache).get_data_service()
    operation_params.dataframe = data
    operation_params.grouping = ["USUBJID"]
    operation_params.filter = {"STUDYID": "CDISC01"}
    result = RecordCount(operation_params, data, cache, data_service).execute()
    assert operation_params.operation_id in result
    assert result.equals(expected)
