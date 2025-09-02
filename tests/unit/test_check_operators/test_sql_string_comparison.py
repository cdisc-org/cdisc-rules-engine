import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)


@pytest.mark.parametrize(
    "data,expected_result",
    [
        (
            # {"target": ["Att", "", None, {None}, {None, 1}, {1, 2}]},
            # [False, True, True, True, False, False],
            {"target": ["Att", "", None]},
            [False, True, True],
        ),
        (
            {"target": [1, 2, None]},
            [False, False, True],
        ),
    ],
)
def test_empty(data, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.empty({"target": "target"})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,expected_result",
    [
        ({"target": ["Att", "", None]}, [True, False, False]),
        ({"target": [1, 2, None]}, [True, True, False]),
    ],
)
def test_non_empty(data, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.non_empty({"target": "target"})
    assert result.equals(pd.Series(expected_result))
