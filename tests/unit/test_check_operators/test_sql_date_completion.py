import pytest
from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


@pytest.mark.parametrize(
    "data,expected_complete",
    [
        (
            {"target": ["2020-01-01", "2020-01", "2020", "08/04", "1987-30"]},
            [True, False, False, False, False],
        ),
        (
            {"target": ["1999-12-31", "2022-89", "2021-05-01", "2021-05", "19-06"]},
            [True, False, True, False, False],
        ),
    ],
)
def test_is_complete_date_sql(data, expected_complete):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result_complete = sql_ops.is_complete_date({"target": "target"})
    assert list(result_complete) == expected_complete


@pytest.mark.parametrize(
    "data,expected_incomplete",
    [
        (
            {"target": ["2020-01-01", "2020-01", "2020", None, ""]},
            [False, True, True, True, True],
        ),
        (
            {"target": ["1999-12-31", "", "2021-05-01", "2021-05", None]},
            [False, True, False, True, True],
        ),
    ],
)
def test_is_incomplete_date_sql(data, expected_incomplete):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result_incomplete = sql_ops.is_incomplete_date({"target": "target"})
    assert list(result_incomplete) == expected_incomplete
