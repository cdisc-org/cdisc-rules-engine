import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)


@pytest.mark.parametrize(
    "data, comparator, within, expected_result",
    [
        (
            {
                "USUBJID": [1, 1, 1, 2, 2, 2],
                "SEQ": [1, 2, 3, 4, 5, 6],
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            1,
            "USUBJID",
            [True, True, True, False, True, True],
        ),
    ],
)
def test_present_on_multiple_rows_within(data, comparator, within, expected_result):
    table_name = "test_present_on_multiple_rows"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.present_on_multiple_rows_within({"target": "target", "comparator": comparator, "within": within})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data, expected_result",
    [
        (
            {
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_different_values(data, expected_result):
    table_name = "test_has_different_values"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.has_different_values({"target": "target"})
    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data, expected_result",
    [
        (
            {
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP3",
                    "AEHOSP2",
                    "AEHOSP2",
                ],
            },
            [False, False, False, False, False, False],
        ),
        (
            {
                "target": [
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                    "AEHOSP1",
                ],
            },
            [True, True, True, True, True, True],
        ),
    ],
)
def test_has_same_values(data, expected_result):
    table_name = "test_has_same_values"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})
    result = sql_ops.has_same_values({"target": "target"})
    assert result.equals(pd.Series(expected_result))
