from unittest.mock import MagicMock

from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset

from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.minus import Minus, _set_difference_preserve_order
import pytest


@pytest.fixture
def minus_params(operation_params: OperationParams) -> OperationParams:
    """Configure operation_params for minus operation tests."""
    operation_params.operation_id = "$expected_minus_dataset"
    operation_params.target = "$expected_variables"
    operation_params.value = "$dataset_variables"
    return operation_params


def test_set_difference_preserve_order():
    """Test set difference preserves order from first list."""
    assert _set_difference_preserve_order(["a", "b", "c"], ["b"]) == ["a", "c"]
    assert _set_difference_preserve_order(["a", "b", "c"], []) == ["a", "b", "c"]
    assert _set_difference_preserve_order(["a", "b", "c"], ["a", "b", "c"]) == []
    assert _set_difference_preserve_order(["A", "B", "C", "D"], ["B", "D"]) == [
        "A",
        "C",
    ]


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_minus_operation(minus_params: OperationParams, dataset_type):
    """Test minus operation computes set difference correctly."""
    eval_dataset = dataset_type.from_dict(
        {
            "$expected_variables": [
                ["STUDYID", "DOMAIN", "AESEQ", "AETERM", "AEDECOD"],
                ["STUDYID", "DOMAIN", "AESEQ", "AETERM", "AEDECOD"],
            ],
            "$dataset_variables": [
                ["STUDYID", "DOMAIN", "AESEQ", "AETERM"],
                ["STUDYID", "DOMAIN", "AESEQ", "AETERM"],
            ],
        }
    )

    operation = Minus(minus_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()

    expected = ["AEDECOD"]  # in expected but not in dataset
    assert list(result[minus_params.operation_id].iloc[0]) == expected


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_minus_empty_value_returns_all_of_name(
    minus_params: OperationParams, dataset_type
):
    """When value is empty, minus returns all of name (per Sam)."""
    eval_dataset = dataset_type.from_dict(
        {
            "$expected_variables": [
                ["A", "B", "C"],
                ["A", "B", "C"],
            ],
            "$dataset_variables": [
                [],
                [],
            ],
        }
    )

    operation = Minus(minus_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()

    assert list(result[minus_params.operation_id].iloc[0]) == ["A", "B", "C"]


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_minus_name_ref_missing_returns_empty(
    minus_params: OperationParams, dataset_type
):
    """When name ref is missing from dataset columns, minus returns empty list."""
    # Dataset has $dataset_variables but not $expected_variables
    eval_dataset = dataset_type.from_dict(
        {
            "$dataset_variables": [
                ["STUDYID", "DOMAIN"],
                ["STUDYID", "DOMAIN"],
            ],
        }
    )

    operation = Minus(minus_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()

    assert list(result[minus_params.operation_id].iloc[0]) == []
