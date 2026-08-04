from unittest.mock import MagicMock

import pytest

from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.operation_params import OperationParams

# Update this import if you choose a different class/module name.
from cdisc_rules_engine.operations.regex_find_replace import RegexFindReplace


@pytest.mark.parametrize(
    "data,find,replace,on_no_match,flags,expected_generated",
    [
        (
            {"variable_name": ["TRT01AN", "TRT99AN"]},
            r"^TRT([0-9]{2})AN$",
            r"TRT\1A",
            "keep_original",
            "",
            ["TRT01A", "TRT99A"],
        ),
        (
            {"variable_name": ["TRT1AN", "TRT001AN", "ABCDE"]},
            r"^TRT([0-9]{2})AN$",
            r"TRT\1A",
            "keep_original",
            "",
            ["TRT1AN", "TRT001AN", "ABCDE"],
        ),
        (
            {"variable_name": ["TRT1AN", "TRT02AN", "ABCDE"]},
            r"^TRT([0-9]{2})AN$",
            r"TRT\1A",
            "set_null",
            "",
            [None, "TRT02A", None],
        ),
        (
            {"variable_name": ["TRT1AN", "TRT02AN", "ABCDE"]},
            r"^TRT([0-9]{2})AN$",
            r"TRT\1A",
            "set_empty",
            "",
            ["", "TRT02A", ""],
        ),
        (
            {"variable_name": [None, "TRT03AN", ""]},
            r"^TRT([0-9]{2})AN$",
            r"TRT\1A",
            "keep_original",
            "",
            [None, "TRT03A", ""],
        ),
        (
            {"variable_name": ["trt04an", "TRT04AN"]},
            r"^TRT([0-9]{2})AN$",
            r"TRT\1A",
            "keep_original",
            "i",
            ["TRT04A", "TRT04A"],
        ),
    ],
)
@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_regex_find_replace_matrix(
    operation_params: OperationParams,
    dataset_type,
    data,
    find,
    replace,
    on_no_match,
    flags,
    expected_generated,
):
    eval_dataset = dataset_type.from_dict(data)

    operation_params.operation_name = "regex_find_replace"
    operation_params.operation_id = "$generated_variable_name"
    operation_params.target = "variable_name"

    # These are expected to be mapped in rule_processor into OperationParams.
    # Assign directly here for unit testing the operation class.
    operation_params.find = find
    operation_params.replace = replace
    operation_params.on_no_match = on_no_match
    operation_params.flags = flags

    operation = RegexFindReplace(
        operation_params,
        eval_dataset,
        MagicMock(),
        MagicMock(),
    )
    result = operation.execute()

    assert operation_params.operation_id in result
    assert result[operation_params.operation_id].equals(
        eval_dataset.convert_to_series(expected_generated)
    )


@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_regex_find_replace_no_match_error_matrix(
    operation_params: OperationParams,
    dataset_type,
):
    eval_dataset = dataset_type.from_dict({"variable_name": ["TRT1AN"]})

    operation_params.operation_name = "regex_find_replace"
    operation_params.operation_id = "$generated_variable_name"
    operation_params.target = "variable_name"
    operation_params.find = r"^TRT([0-9]{2})AN$"
    operation_params.replace = r"TRT\1A"
    operation_params.on_no_match = "error"
    operation_params.flags = ""

    operation = RegexFindReplace(
        operation_params,
        eval_dataset,
        MagicMock(),
        MagicMock(),
    )
    with pytest.raises(Exception, match="no match|No match|on_no_match"):
        operation.execute()


@pytest.mark.parametrize(
    "overrides,expected_error_match",
    [
        ({"operation_id": None}, "id|operation_id"),
        ({"target": None}, "name|target"),
        ({"find": None}, "find|regex"),
        ({"replace": None}, "replace"),
        ({"find": r"^TRT([0-9]{2}AN$"}, "regex|pattern"),
        ({"on_no_match": "bad_value"}, "on_no_match"),
        ({"flags": "z"}, "flags"),
        ({"target": "missing_column"}, "missing_column|target"),
    ],
)
@pytest.mark.parametrize("dataset_type", [PandasDataset, DaskDataset])
def test_regex_find_replace_validation_matrix(
    operation_params: OperationParams,
    dataset_type,
    overrides,
    expected_error_match,
):
    eval_dataset = dataset_type.from_dict({"variable_name": ["TRT01AN"]})

    operation_params.operation_name = "regex_find_replace"
    operation_params.operation_id = "$generated_variable_name"
    operation_params.target = "variable_name"
    operation_params.find = r"^TRT([0-9]{2})AN$"
    operation_params.replace = r"TRT\1A"
    operation_params.on_no_match = "keep_original"
    operation_params.flags = ""

    for key, value in overrides.items():
        setattr(operation_params, key, value)

    operation = RegexFindReplace(
        operation_params,
        eval_dataset,
        MagicMock(),
        MagicMock(),
    )
    with pytest.raises(Exception, match=expected_error_match):
        operation.execute()
