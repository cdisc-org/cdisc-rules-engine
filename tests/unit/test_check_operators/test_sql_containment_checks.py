import pandas as pd
import pytest

from .helpers import create_sql_operators, assert_series_equals

CONTAINED_BY_TEST_DATA = [
    (
        {"target": ["Ctt", "Btt", "A"]},
        ["Ctt", "B", "A"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"]},
        ["C", "Z", "A"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"], "VAR2": ["A", "B", "D"]},
        "VAR2",
        False,
        [True, True, False],
    ),
    (
        {"target": ["A", "B", "C"]},
        "B",
        True,
        [False, True, False],
    ),
    # Note: Doesn't seem like there is a way to test this using SQL
    # (
    #     {"target": [1, 2, 3], "VAR2": [[1, 2], [3], [3]]},
    #     "VAR2",
    #     [True, False, True],
    # ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINED_BY_TEST_DATA,
)
def test_is_contained_by(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_contained_by(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINED_BY_TEST_DATA,
)
def test_is_not_contained_by(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_not_contained_by(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, ~pd.Series(expected_result))


CONTAINED_BY_CASE_INSENSITIVE_TEST_DATA = [
    (
        {"target": ["Ctt", "Btt", "A"]},
        ["ctt", "b", "a"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"]},
        ["c", "z", "a"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"]},
        "b",
        True,
        [False, True, False],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINED_BY_CASE_INSENSITIVE_TEST_DATA,
)
def test_is_contained_by_case_insensitive(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_contained_by_case_insensitive(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINED_BY_CASE_INSENSITIVE_TEST_DATA,
)
def test_is_not_contained_by_case_insensitive(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_not_contained_by_case_insensitive(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, ~pd.Series(expected_result))
