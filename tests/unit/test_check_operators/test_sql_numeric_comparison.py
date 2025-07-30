import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import SQLDataframeType
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            True,
            [True, True, False],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            2,
            False,
            [True, False, False],
        ),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            True,
            [True, True, False],
        ),
    ],
)
def test_sql_less_than(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = SQLDataframeType({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.less_than({"target": "target", "comparator": comparator, "value_is_literal": value_is_literal})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            True,
            [True, True, True],
        ),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, False, [True, True, False]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            True,
            [True, True, True],
        ),
    ],
)
def test_sql_less_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = SQLDataframeType({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.less_than_or_equal_to(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": [1, 2, 4], "VAR2": [3, 3, 3]},
            "VAR2",
            True,
            [False, False, True],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            2,
            False,
            [False, False, True],
        ),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            True,
            [False, False, False],
        ),
    ],
)
def test_sql_greater_than(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = SQLDataframeType({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.greater_than({"target": "target", "comparator": comparator, "value_is_literal": value_is_literal})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            True,
            [False, False, True],
        ),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, False, [False, True, True]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            True,
            [False, False, True],
        ),
    ],
)
def test_sql_greater_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = SQLDataframeType({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.greater_than_or_equal_to(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert result.equals(pd.Series(expected_result))
