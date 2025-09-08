import pandas as pd
import pytest

from .helpers import assert_series_equals, create_sql_operators

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
    (
        {"target": ["AAA", "BAB", "CAC"]},
        "$list",
        False,
        1,
        [True, True, False],
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
    (
        {"target": ["AAA", "BAB", "CAC"]},
        "$list",
        False,
        1,
        [True, True, False],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    PREFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_prefix_is_contained_by(data, comparator, value_is_literal, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.prefix_is_contained_by(
        {
            "target": "target",
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    PREFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_prefix_is_not_contained_by(data, comparator, value_is_literal, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.prefix_is_not_contained_by(
        {
            "target": "target",
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, ~pd.Series(expected_result))


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    SUFFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_suffix_is_contained_by(data, comparator, value_is_literal, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.suffix_is_contained_by(
        {
            "target": "target",
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    SUFFIX_IS_CONTAINED_BY_TEST_DATA,
)
def test_suffix_is_not_contained_by(data, comparator, value_is_literal, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.suffix_is_not_contained_by(
        {
            "target": "target",
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, ~pd.Series(expected_result))
