from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.intersect import Intersect, _set_intersection


@pytest.fixture
def intersect_params(operation_params: OperationParams) -> OperationParams:
    operation_params.operation_id = "$library_with_dataset_only"
    operation_params.target = "$column_order_from_library"
    operation_params.subtract = "$column_order_from_dataset"
    return operation_params


@pytest.mark.parametrize(
    "list_a,list_b,case_sensitive,expected",
    [
        (["a", "b", "c"], ["b"], True, ["b"]),
        (["a", "b", "c"], ["c", "a"], True, ["a", "c"]),
        (["a", "b", "c"], [], True, []),
        ([], ["a"], True, []),
        (["a", "b", "b", "a"], ["a", "b"], True, ["a", "b"]),
        (["a", None, "b"], [None, "b"], True, ["b"]),
        ("x", ["x"], True, ["x"]),
        (["x"], "x", True, ["x"]),
        (["aeterm", "AESEQ"], ["AETERM"], True, []),
        (["aeterm", "AESEQ"], ["AETERM"], False, ["AETERM"]),
        (["a", "A"], ["a"], False, ["A"]),
    ],
)
def test_set_intersection(list_a, list_b, case_sensitive, expected):
    assert _set_intersection(list_a, list_b, case_sensitive) == expected


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_intersect_operation(intersect_params: OperationParams, dataset_type):
    eval_dataset = dataset_type.from_dict(
        {
            "$column_order_from_library": [
                ["STUDYID", "DOMAIN", "USUBJID", "AESEQ", "AETERM", "AEDECOD"],
                ["STUDYID", "DOMAIN", "USUBJID", "AESEQ", "AETERM", "AEDECOD"],
            ],
            "$column_order_from_dataset": [
                ["STUDYID", "AETERM", "DOMAIN", "AESEQ", "AEXTRA"],
                ["STUDYID", "AETERM", "DOMAIN", "AESEQ", "AEXTRA"],
            ],
        }
    )
    operation = Intersect(intersect_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()
    assert list(result[intersect_params.operation_id].iloc[0]) == [
        "STUDYID",
        "DOMAIN",
        "AESEQ",
        "AETERM",
    ]


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_intersect_case_insensitive(intersect_params: OperationParams, dataset_type):
    eval_dataset = dataset_type.from_dict(
        {
            "$column_order_from_library": [
                ["STUDYID", "AETERM"],
                ["STUDYID", "AETERM"],
            ],
            "$column_order_from_dataset": [
                ["studyid", "aeterm"],
                ["studyid", "aeterm"],
            ],
        }
    )
    intersect_params.case_sensitive = False
    operation = Intersect(intersect_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()
    assert list(result[intersect_params.operation_id].iloc[0]) == ["STUDYID", "AETERM"]


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_intersect_missing_subtract_returns_empty(
    intersect_params: OperationParams, dataset_type
):
    eval_dataset = dataset_type.from_dict(
        {"$column_order_from_library": [["STUDYID", "DOMAIN"], ["STUDYID", "DOMAIN"]]}
    )
    operation = Intersect(intersect_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()
    assert list(result[intersect_params.operation_id].iloc[0]) == []


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_intersect_missing_name_returns_empty(
    intersect_params: OperationParams, dataset_type
):
    eval_dataset = dataset_type.from_dict(
        {"$column_order_from_dataset": [["STUDYID", "DOMAIN"], ["STUDYID", "DOMAIN"]]}
    )
    operation = Intersect(intersect_params, eval_dataset, MagicMock(), MagicMock())
    result = operation.execute()
    assert list(result[intersect_params.operation_id].iloc[0]) == []
