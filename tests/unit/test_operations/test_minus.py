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
    operation_params.subtract = "$dataset_variables"
    return operation_params


@pytest.mark.parametrize(
    "list_a,list_b,expected",
    [
        (["a", "b", "c"], ["b"], ["a", "c"]),
        (["a", "b", "c"], [], ["a", "b", "c"]),
        (["a", "b", "c"], ["a", "b", "c"], []),
        (["A", "B", "C", "D"], ["B", "D"], ["A", "C"]),
        (["a", "b", "b", "c"], ["b"], ["a", "c"]),
        (["a", "a", "b"], ["b"], ["a", "a"]),
        ("x", ["a"], ["x"]),
        (["x"], "a", ["x"]),
        ("a", "b", ["a"]),
        (["a", "", "b"], [""], ["a", "b"]),
        (["a", "", "b"], ["c"], ["a", "", "b"]),
        ([""], [""], []),
    ],
)
def test_set_difference_preserve_order(list_a, list_b, expected):
    assert _set_difference_preserve_order(list_a, list_b) == expected


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_minus_operation(minus_params: OperationParams, dataset_type):
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
    assert list(result[minus_params.operation_id].iloc[0]) == ["AEDECOD"]


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_minus_empty_subtract_returns_all_of_name(
    minus_params: OperationParams, dataset_type
):
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


@pytest.mark.parametrize(
    "expected_vars,dataset_vars,expected",
    [
        ([["a", "b", "b", "c"], ["a", "b", "b", "c"]], [["b"], ["b"]], ["a", "c"]),
        ([["a", "a", "b"], ["a", "a", "b"]], [["b"], ["b"]], ["a", "a"]),
        ([["a", "", "b"], ["a", "", "b"]], [[""], [""]], ["a", "b"]),
        ([["a", "", "b"], ["a", "", "b"]], [["c"], ["c"]], ["a", "", "b"]),
        ([[""], [""]], [[""], [""]], []),
        (["x", "y"], [["a"], ["a"]], ["x"]),
    ],
)
@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_minus_operation_edge_cases(
    minus_params: OperationParams,
    dataset_type,
    expected_vars,
    dataset_vars,
    expected,
):
    eval_dataset = dataset_type.from_dict(
        {
            "$expected_variables": expected_vars,
            "$dataset_variables": dataset_vars,
        }
    )
    operation = Minus(minus_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()
    assert list(result[minus_params.operation_id].iloc[0]) == expected


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_minus_name_ref_missing_returns_empty(
    minus_params: OperationParams, dataset_type
):
    eval_dataset = dataset_type.from_dict(
        {"$dataset_variables": [["STUDYID", "DOMAIN"], ["STUDYID", "DOMAIN"]]}
    )
    operation = Minus(minus_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()
    assert list(result[minus_params.operation_id].iloc[0]) == []
