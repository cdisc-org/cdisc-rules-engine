import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult


@pytest.mark.parametrize(
    "data,comparator,is_literal,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            False,
            [False, True, False],
        ),
        (
            {"target": ["A", "B", "VAR2"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            True,
            [False, False, True],
        ),
        (
            {"target": [2, 1.0, 1]},
            1,
            True,
            [False, True, True],
        ),
        (
            {"target": ["A", "B", ""], "VAR2": ["", "", ""]},
            "VAR2",
            False,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", None], "VAR2": ["A", "B", "C"]},
            "",
            True,
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"]},
            "$value",
            False,
            [True, False, False],
        ),
    ],
)
def test_equal_to(data, comparator, is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators(
        {
            "validation_dataset_id": table_name,
            "sql_data_service": tds,
            "operation_variables": {"$value": SqlOperationResult(query="SELECT 'A'", type="constant")},
        }
    )
    result = sql_ops.equal_to({"target": "target", "comparator": comparator, "value_is_literal": is_literal})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,operator,expected_result",
    [
        (
            {
                "target": ["A", "B", "C"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": ["D", "D", "C"],
                "AESEQ": ["E", "B", "E"],
            },
            "IDVAR",
            "equal_to",
            [False, True, True],
        ),
        (
            {
                "target": ["A", "B", "C"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": ["D", "D", "C"],
                "AESEQ": ["E", "B", "E"],
            },
            "IDVAR",
            "not_equal_to",
            [True, False, False],
        ),
    ],
)
def test_equality_operators_value_is_reference(data, comparator, operator, expected_result):
    """Test equal_to and not_equal_to operators with value_is_reference=True for dynamic column comparison."""
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    if operator == "equal_to":
        result = sql_ops.equal_to({"target": "target", "comparator": comparator, "value_is_reference": True})
    else:
        result = sql_ops.not_equal_to({"target": "target", "comparator": comparator, "value_is_reference": True})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,operator,expected_result",
    [
        (
            {
                "target": ["320", "2", "15"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            [True, True, True],
        ),
        (
            {
                "target": ["320", "2", "15"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "not_equal_to",
            [False, False, False],
        ),
        (
            {
                "target": ["999", "5", "100"],
                "IDVAR": ["LBSEQ", "AESEQ", "LBSEQ"],
                "LBSEQ": [320, 21, 15],
                "AESEQ": [1, 2, 1],
            },
            "IDVAR",
            "equal_to",
            [False, False, False],
        ),
        (
            {
                "target": ["1", "2", "3"],
                "IDVAR": ["FLOATCOL", "FLOATCOL", "FLOATCOL"],
                "FLOATCOL": [1.0, 2.0, 3.0],
            },
            "IDVAR",
            "equal_to",
            [True, True, True],
        ),
    ],
)
def test_equality_operators_type_insensitive(data, comparator, operator, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    if operator == "equal_to":
        result = sql_ops.equal_to(
            {
                "target": "target",
                "comparator": comparator,
                "value_is_reference": True,
                "type_insensitive": True,
            }
        )
    else:
        result = sql_ops.not_equal_to(
            {
                "target": "target",
                "comparator": comparator,
                "value_is_reference": True,
                "type_insensitive": True,
            }
        )

    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "VAR2",
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "B",
            [True, False, True],
        ),
        (
            {"target": ["A", "a", "b"]},
            "$value",
            [False, True, True],
        ),
    ],
)
def test_not_equal_to(data, comparator, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators(
        {
            "validation_dataset_id": table_name,
            "sql_data_service": tds,
            "operation_variables": {"$value": SqlOperationResult(query="SELECT 'A'", type="constant")},
        }
    )
    result = sql_ops.not_equal_to({"target": "target", "comparator": comparator})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["a", "b", "c"]},
            "VAR2",
            [True, True, True],
        ),
        (
            {"target": ["A", "b", "B"], "VAR2": ["A", "B", "C"]},
            "B",
            [False, True, True],
        ),
        (
            {"target": ["A", "a", "b"]},
            "$value",
            [True, True, False],
        ),
    ],
)
def test_equal_to_case_insensitive(data, comparator, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators(
        {
            "validation_dataset_id": table_name,
            "sql_data_service": tds,
            "operation_variables": {"$value": SqlOperationResult(query="SELECT 'A'", type="constant")},
        }
    )
    result = sql_ops.equal_to_case_insensitive({"target": "target", "comparator": comparator})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": ["A", "B", "C"], "VAR2": ["a", "b", "c"]},
            "VAR2",
            [False, False, False],
        ),
        (
            {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
            "b",
            [True, False, True],
        ),
        (
            {"target": ["A", "a", "b"]},
            "$value",
            [False, False, True],
        ),
    ],
)
def test_not_equal_to_case_insensitive(data, comparator, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators(
        {
            "validation_dataset_id": table_name,
            "sql_data_service": tds,
            "operation_variables": {"$value": SqlOperationResult(query="SELECT 'A'", type="constant")},
        }
    )
    result = sql_ops.not_equal_to_case_insensitive({"target": "target", "comparator": comparator})
    assert result.equals(pd.Series(expected_result))
