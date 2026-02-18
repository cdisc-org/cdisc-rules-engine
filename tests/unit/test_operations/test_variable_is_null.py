from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.variable_is_null import VariableIsNull
from cdisc_rules_engine.models.operation_params import OperationParams
import pytest
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory


@pytest.mark.parametrize(
    "data, target_var, expected",
    [
        (
            PandasDataset.from_dict({"VAR1": ["A", "B", "C"], "VAR2": [1, 2, 3]}),
            "VAR1",
            False,
        ),
        (
            PandasDataset.from_dict({"VAR1": ["", None, "C"], "VAR2": [1, 2, 3]}),
            "VAR1",
            False,
        ),
        (
            DaskDataset.from_dict({"VAR1": ["", None, "C"], "VAR2": [1, 2, 3]}),
            "VAR1",
            False,
        ),
        (
            PandasDataset.from_dict({"VAR1": ["", None, ""], "VAR2": [1, 2, 3]}),
            "VAR1",
            True,
        ),
        (
            DaskDataset.from_dict({"VAR1": ["", None, ""], "VAR2": [1, 2, 3]}),
            "VAR1",
            True,
        ),
        (
            PandasDataset.from_dict(
                {"VAR1": [None, None, None, "X"], "VAR2": [1, 2, 3, 4]}
            ),
            "VAR1",
            False,
        ),
        (
            PandasDataset.from_dict(
                {"VAR1": ["", "", "", "data"], "VAR2": [1, 2, 3, 4]}
            ),
            "VAR1",
            False,
        ),
        (
            PandasDataset.from_dict({"VAR1": [None, None, None], "VAR2": [1, 2, 3]}),
            "VAR1",
            True,
        ),
        (
            PandasDataset.from_dict({"VAR2": ["A", "B", "C"]}),
            "VAR1",
            True,
        ),
        (
            DaskDataset.from_dict({"VAR2": ["A", "B", "C"]}),
            "VAR1",
            True,
        ),
        (
            PandasDataset.from_dict({"VAR2": ["A", "B", "C"]}),
            "NONEXISTENT",
            True,
        ),
    ],
)
def test_variable_is_null_submission(
    data, target_var, expected, mock_data_service, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = data
    operation_params.target = target_var
    operation_params.domain = "AE"
    operation_params.source = "submission"
    operation_params.level = "dataset"
    mock_data_service.get_dataset.return_value = data
    mock_data_service.dataset_implementation = data.__class__
    result = VariableIsNull(operation_params, data, cache, mock_data_service).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected


def test_variable_is_null_evaluation_dataset_level(mock_data_service, operation_params):
    evaluation_dataset = PandasDataset.from_dict({"VAR1": [None, None], "VAR2": [1, 2]})
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = evaluation_dataset
    operation_params.target = "VAR1"
    operation_params.source = "evaluation"
    operation_params.level = "dataset"
    mock_data_service.dataset_implementation = PandasDataset
    result = VariableIsNull(
        operation_params, evaluation_dataset, cache, mock_data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val is True


def test_variable_is_null_evaluation_row_level(mock_data_service, operation_params):
    evaluation_dataset = PandasDataset.from_dict(
        {"VAR1": [None, "A", None, "B"], "VAR2": [1, 2, 3, 4]}
    )
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = evaluation_dataset
    operation_params.target = "VAR1"
    operation_params.source = "evaluation"
    operation_params.level = "row"
    mock_data_service.dataset_implementation = PandasDataset
    result = VariableIsNull(
        operation_params, evaluation_dataset, cache, mock_data_service
    ).execute()
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].to_list() == [True, False, True, False]


def test_variable_is_null_row_level_empty_string(mock_data_service, operation_params):
    """Test row-level check treats empty string as null."""
    evaluation_dataset = PandasDataset.from_dict(
        {"VAR1": ["", "A", None, "B"], "VAR2": [1, 2, 3, 4]}
    )
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = evaluation_dataset
    operation_params.target = "VAR1"
    operation_params.source = "evaluation"
    operation_params.level = "row"
    mock_data_service.dataset_implementation = PandasDataset
    result = VariableIsNull(
        operation_params, evaluation_dataset, cache, mock_data_service
    ).execute()
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].to_list() == [True, False, True, False]


def test_variable_is_null_row_level_missing_variable(
    mock_data_service, operation_params
):
    """Test row-level check returns all True when variable is missing from dataset."""
    evaluation_dataset = PandasDataset.from_dict({"VAR2": ["A", "B", "C"]})
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = evaluation_dataset
    operation_params.target = "VAR1"
    operation_params.source = "evaluation"
    operation_params.level = "row"
    mock_data_service.dataset_implementation = PandasDataset
    result = VariableIsNull(
        operation_params, evaluation_dataset, cache, mock_data_service
    ).execute()
    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].to_list() == [True, True, True]


def test_variable_is_null_row_level_raises_for_submission(
    mock_data_service, operation_params
):
    """Test that level=row raises an error when source=submission."""
    data = PandasDataset.from_dict({"VAR1": ["A", "B", "C"]})
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    operation_params.dataframe = data
    operation_params.target = "VAR1"
    operation_params.source = "submission"
    operation_params.level = "row"
    mock_data_service.get_dataset.return_value = data
    mock_data_service.dataset_implementation = PandasDataset
    with pytest.raises(
        ValueError, match="level: row may only be used with source: evaluation"
    ):
        VariableIsNull(operation_params, data, cache, mock_data_service).execute()
