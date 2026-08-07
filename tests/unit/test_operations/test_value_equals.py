import pytest
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.value_equals import ValueEquals
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_value_equals_single_target_returns_matching_variable_names(
    dataset_type, mock_data_service, operation_params: OperationParams
):
    data = dataset_type.from_dict({"PPSTRESU": ["3", "4", "3"]})
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()

    operation_params.dataframe = data
    operation_params.target = "PPSTRESU"
    operation_params.value = "3"

    result = ValueEquals(operation_params, data, cache, mock_data_service).execute()

    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].tolist() == [
        ["PPSTRESU"],
        [],
        ["PPSTRESU"],
    ]


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_value_equals_null_matches_only_true_null(
    dataset_type, mock_data_service, operation_params: OperationParams
):
    data = dataset_type.from_dict({"PPSTRESU": [None, "", "3"]})
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()

    operation_params.dataframe = data
    operation_params.target = "PPSTRESU"
    operation_params.value = None

    result = ValueEquals(operation_params, data, cache, mock_data_service).execute()

    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].tolist() == [
        ["PPSTRESU"],  # true null -> match
        [],            # empty string -> no match
        [],            # non-null value -> no match
    ]


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_value_equals_variable_list_target_returns_only_matching_variables(
    dataset_type, mock_data_service, operation_params: OperationParams
):
    data = dataset_type.from_dict(
        {
            "$var_list": [["A", "B"], ["A", "B"], ["A", "B"]],
            "A": [1, 3, 3],
            "B": [3, 2, 3],
        }
    )
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()

    operation_params.dataframe = data
    operation_params.target = "$var_list"
    operation_params.value = 3

    result = ValueEquals(operation_params, data, cache, mock_data_service).execute()

    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].tolist() == [
        ["B"],       # row 1: A=1, B=3
        ["A"],       # row 2: A=3, B=2
        ["A", "B"],  # row 3: A=3, B=3
    ]