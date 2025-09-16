import pandas as pd
import pytest

from .helpers import assert_series_equals, create_sql_operators

PREFIX_EQUAL_TO_TEST_DATA = [
    (
        {"target": ["testA", "B", "testC"], "VAR2": ["test", "test", "test"]},
        "VAR2",
        False,
        4,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
        "VAR2",
        False,
        1,
        [True, True, True],
    ),
    (
        {"target": ["testA", "demoB", "testC"]},
        ["test", "demo"],
        True,
        4,
        [True, True, True],
    ),
    (
        {"target": ["testA", "B", "C"]},
        "test",
        True,
        4,
        [True, False, False],
    ),
    (
        {"target": ["Ctt", "Btt", "A"]},
        "$constant",
        False,
        1,
        [False, False, True],
    ),
    (
        {"target": ["AAA", "BAB", "CAC"]},
        "$list",
        False,
        1,
        [True, True, False],
    ),
    (
        {"target": ["testA", "B", None], "VAR2": ["test", "test", "test"]},
        "VAR2",
        False,
        2,
        [False, False, False],
    ),
    (
        {"target": ["testA", "", "testC"], "VAR2": ["", "", ""]},
        "VAR2",
        False,
        4,
        [False, False, False],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    PREFIX_EQUAL_TO_TEST_DATA,
)
def test_prefix_equal_to(data, comparator, value_is_literal, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.prefix_equal_to(
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
    PREFIX_EQUAL_TO_TEST_DATA,
)
def test_prefix_not_equal_to(data, comparator, value_is_literal, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.prefix_not_equal_to(
        {
            "target": "target",
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, ~pd.Series(expected_result))


SUFFIX_EQUAL_TO_TEST_DATA = [
    (
        {"target": ["Atest", "B", "Ctest"], "VAR2": ["test", "test", "test"]},
        "VAR2",
        False,
        4,
        [True, False, True],
    ),
    (
        {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
        "VAR2",
        False,
        1,
        [True, True, True],
    ),
    (
        {"target": ["Atest", "Bdemo", "Ctest"]},
        ["test", "demo"],
        True,
        4,
        [True, True, True],
    ),
    (
        {"target": ["Atest", "B", "C"]},
        "test",
        True,
        4,
        [True, False, False],
    ),
    (
        {"target": ["Ctt", "Btt", "A"]},
        "$constant",
        False,
        1,
        [False, False, True],
    ),
    (
        {"target": ["AAA", "BAB", "CAC"]},
        "$list",
        False,
        1,
        [True, True, False],
    ),
    (
        {"target": ["Atest", "B", None], "VAR2": ["test", "test", "test"]},
        "VAR2",
        False,
        2,
        [False, False, False],
    ),
    (
        {"target": ["Atest", "", "Ctest"], "VAR2": ["", "", ""]},
        "VAR2",
        False,
        4,
        [False, False, False],
    ),
    (
        {"target": ["Atest", "B", "Ctest"]},
        "$constant",
        False,
        4,
        [False, False, False],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,length,expected_result",
    SUFFIX_EQUAL_TO_TEST_DATA,
)
def test_suffix_equal_to(data, comparator, value_is_literal, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.suffix_equal_to(
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
    SUFFIX_EQUAL_TO_TEST_DATA,
)
def test_suffix_not_equal_to(data, comparator, value_is_literal, length, expected_result):
    sql_ops = create_sql_operators(data)

    result = sql_ops.suffix_not_equal_to(
        {
            "target": "target",
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, ~pd.Series(expected_result))
