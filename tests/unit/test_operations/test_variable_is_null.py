from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import dask.dataframe as dd
import pytest
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict({"AEVAR": ["A", "B", "C"]}),
            False,
        ),
        (
            pd.DataFrame.from_dict({"AEVAR": [1, 2, 3]}),
            False,
        ),
        (
            pd.DataFrame.from_dict({"AEVAR": ["", None, "C"]}),
            False,
        ),
        (
            pd.DataFrame.from_dict({"AEVAR": [None, None, 3]}),
            False,
        ),
        (
            pd.DataFrame.from_dict({"AEVAR": ["", None]}),
            True,
        ),
        (
            pd.DataFrame.from_dict({"BCVAR": ["A", "B", "C"]}),
            True,
        ),
        (
            dd.DataFrame.from_dict({"AEVAR": ["A", "B", "C"]}, npartitions=1),
            False,
        ),
        (
            dd.DataFrame.from_dict({"AEVAR": [1, 2, 3]}, npartitions=1),
            False,
        ),
        (
            dd.DataFrame.from_dict({"AEVAR": ["", None, "C"]}, npartitions=1),
            False,
        ),
        (
            dd.DataFrame.from_dict({"AEVAR": [None, None, 3]}, npartitions=1),
            False,
        ),
        (
            dd.DataFrame.from_dict({"AEVAR": ["", None]}, npartitions=1),
            True,
        ),
        (
            dd.DataFrame.from_dict({"BCVAR": ["A", "B", "C"]}, npartitions=1),
            True,
        ),
    ],
)
def test_variable_is_null(
    data, expected, mock_data_service, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = data
    operation_params.target = "--VAR"
    operation_params.domain = "AE"
    mock_data_service.get_dataset.return_value = data
    operations = DatasetOperations()
    result = operations.get_service(
        "variable_is_null",
        operation_params,
        data,
        cache,
        mock_data_service,
    )
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected


def test_define_crosscheck_variable_is_null(mock_data_service, operation_params):
    define_metadata = pd.DataFrame.from_dict(
        {
            "define_variable_name": ["AEHLT", "AETERM"],
            "define_variable_has_no_data": ["Yes", "No"],
        }
    )
    dataset = pd.DataFrame.from_dict({"AEHLT": [None, None], "AETERM": [1, 2]})
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = define_metadata
    operation_params.target = "define_variable_name"
    mock_data_service.get_dataset.return_value = dataset
    operations = DatasetOperations()
    result = operations.get_service(
        "variable_is_null",
        operation_params,
        pd.DataFrame.copy(define_metadata),
        cache,
        mock_data_service,
    )
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].to_list() == [True, False]


def test_define_crosscheck_variable_is_null_dask(mock_data_service, operation_params):
    define_metadata = pd.DataFrame.from_dict(
        {
            "define_variable_name": ["AEHLT", "AETERM"],
            "define_variable_has_no_data": ["Yes", "No"],
        }
    )
    dataset = dd.DataFrame.from_dict(
        {"AEHLT": [None, None], "AETERM": [1, 2]}, npartitions=1
    )
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = define_metadata
    operation_params.target = "define_variable_name"
    mock_data_service.get_dataset.return_value = dataset
    operations = DatasetOperations()
    result = operations.get_service(
        "variable_is_null",
        operation_params,
        pd.DataFrame.copy(define_metadata),
        cache,
        mock_data_service,
    )
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].to_list() == [True, False]
