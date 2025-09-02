import pandas as pd
import pytest

from cdisc_rules_engine.check_operators.sql_operators import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)

PREFIX_IS_CONTAINED_BY_TEST_DATA = [
    (
        {"target": ["AETEST", "AETESTCD", "LBTEST"], "domain_col": ["AE", "XX", "RR"]},
        "domain_col",
        False,
        2,
        [True, True, False],
    ),
    (
        {"target": ["AETEST", "AFTESTCD", "RRTEST"], "domain_col": ["AE", "XX", "RR"]},
        "domain_col",
        False,
        2,
        [True, False, True],
    ),
    (
        {"target": ["AETEST", "AETESTCD", "LBTEST"]},
        ["AE", "LB"],
        True,
        2,
        [True, True, True],
    ),
    (
        {"target": ["AETEST", "AFTESTCD", "RRTEST"]},
        ["AE", "RR"],
        True,
        2,
        [True, False, True],
    ),
    # Note: This pattern works in DataFrame tests but fails in SQL:
    # (
    #     {
    #         "target": ["AETEST", "AETESTCD", "LBTEST"],
    #         "study_domains": [
    #             ["DM", "AE", "LB", "TV"],  # List per row - not supported in SQL
    #             ["DM", "AE", "LB", "TV"],
    #             ["DM", "AE", "LB", "TV"],
    #         ]
    #     },
    #     "study_domains",
    #     False,
    #     2,
    #     [True, True, True]
    # ),
]

SUFFIX_IS_CONTAINED_BY_TEST_DATA = [
    (
        {"target": ["AETEST", "AETESTCD", "LBTEGG"], "suffix_col": ["ST", "CD", "LE"]},
        "suffix_col",
        False,
        2,
        [True, True, False],
    ),
    (
        {"target": ["AETEST", "AFTESTCD", "RRTELE"], "suffix_col": ["ST", "CD", "LE"]},
        "suffix_col",
        False,
        2,
        [True, True, True],
    ),
    (
        {"target": ["AETEST", "AETESTCD", "LBTEGG"]},
        ["ST", "CD", "GG"],
        True,
        2,
        [True, True, True],
    ),
    (
        {"target": ["AETEST", "AFTESTCD", "CRTELE"]},
        ["ST", "CD"],
        True,
        2,
        [True, True, False],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    PREFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_prefix_is_contained_by(data, comparator, value_is_literal, length, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.prefix_is_contained_by(
        {
            "target": "target",
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    PREFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_prefix_is_not_contained_by(data, comparator, value_is_literal, length, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.prefix_is_not_contained_by(
        {
            "target": "target",
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert result.equals(~pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    SUFFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_suffix_is_contained_by(data, comparator, value_is_literal, length, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.suffix_is_contained_by(
        {
            "target": "target",
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert result.equals(pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    SUFFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_suffix_is_not_contained_by(data, comparator, value_is_literal, length, expected_result):
    table_name = "test_table"
    tds = PostgresQLDataService.from_column_data(table_name=table_name, column_data=data)
    sql_ops = PostgresQLOperators({"validation_dataset_id": table_name, "sql_data_service": tds})

    result = sql_ops.suffix_is_not_contained_by(
        {
            "target": "target",
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert result.equals(~pd.Series(expected_result))
