import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)


@pytest.mark.parametrize(
    "target, expected_result",
    [
        ("var1", [True, True, True]),
        ("--r1", [True, True, True]),
        # ("nested_var", [True, True, True]),
        ("invalid", [False, False, False]),
        # ("a", [True, True, True]),
        # ("f", [True, True, True]),
        ("x", [False, False, False]),
        ("non_nested_value", [True, True, True]),
    ],
)
def test_exists(target, expected_result):
    data = {
        "var1": [1, 2, 4],
        "var2": [3, 5, 6],
        # "nested_var": [["a", "b", "c"], ["d", "e"], ["f", "nested_var", "g"]],
        "non_nested_value": ["h", "i", "j"],
    }
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators(
        {"validation_dataset_id": table_name, "sql_data_service": tds, "column_prefix_map": {"--": "va"}}
    )
    result = sql_ops.exists({"target": target})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "target, expected_result",
    [
        ("var1", [False, False, False]),
        ("--r1", [False, False, False]),
        # ("nested_var", [False, False, False]),
        ("invalid", [True, True, True]),
        # ("a", [False, False, False]),
        # ("f", [False, False, False]),
        ("x", [True, True, True]),
        ("non_nested_value", [False, False, False]),
    ],
)
def test_not_exists(target, expected_result):
    data = {
        "var1": [1, 2, 4],
        "var2": [3, 5, 6],
        # "nested_var": [["a", "b", "c"], ["d", "e"], ["f", "nested_var", "g"]],
        "non_nested_value": ["h", "i", "j"],
    }
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators(
        {"validation_dataset_id": table_name, "sql_data_service": tds, "column_prefix_map": {"--": "va"}}
    )
    result = sql_ops.not_exists({"target": target})
    assert result.equals(pd.Series(expected_result))
