import pandas as pd
import pytest

from .helpers import assert_series_equals, create_sql_operators

PREFIX_EQUAL_TO_TEST_DATA = [
    (
        {"target": ["testA", "B", "testC"], "VAR2": ["test", "test", "test"]},
        "target",
        "VAR2",
        False,
        4,
        [True, False, True],
        None,
    ),
    (
        {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
        "target",
        "VAR2",
        False,
        1,
        [True, True, True],
        None,
    ),
    (
        {"target": ["testA", "demoB", "testC"]},
        "target",
        ["test", "demo"],
        True,
        4,
        [True, True, True],
        None,
    ),
    (
        {"target": ["testA", "B", "C"]},
        "target",
        "test",
        True,
        4,
        [True, False, False],
        None,
    ),
    (
        {"target": ["Ctt", "Btt", "A"]},
        "target",
        "$constant",
        False,
        1,
        [False, False, True],
        None,
    ),
    (
        {"target": ["AAA", "BAB", "CAC"]},
        "target",
        "$list",
        False,
        1,
        [True, True, False],
        None,
    ),
    (
        {"target": ["testA", "B", None], "VAR2": ["test", "test", "test"]},
        "target",
        "VAR2",
        False,
        2,
        [False, False, False],
        None,
    ),
    (
        {"target": ["testA", "", "testC"], "VAR2": ["", "", ""]},
        "target",
        "VAR2",
        False,
        4,
        [False, False, False],
        None,
    ),
    # Dataset name test cases
    (
        {"dummy_col": ["val1", "val2", "val3"]},
        "dataset_name",
        "TEST",
        True,
        4,
        [True, True, True],
        "TESTDATA",
    ),
    (
        {"dummy_col": ["val1", "val2"]},
        "dataset_name",
        "TEST",
        True,
        3,
        [False, False],
        "TESTDATA",
    ),
]


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,length,expected_result,dataset_name",
    PREFIX_EQUAL_TO_TEST_DATA,
)
def test_prefix_equal_to(data, target, comparator, value_is_literal, length, expected_result, dataset_name):
    sql_ops = create_sql_operators(data, dataset_name=dataset_name)

    result = sql_ops.prefix_equal_to(
        {
            "target": target,
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,length,expected_result,dataset_name",
    PREFIX_EQUAL_TO_TEST_DATA,
)
def test_prefix_not_equal_to(data, target, comparator, value_is_literal, length, expected_result, dataset_name):
    sql_ops = create_sql_operators(data, dataset_name=dataset_name)

    result = sql_ops.prefix_not_equal_to(
        {
            "target": target,
            "comparator": comparator,
            "prefix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, ~pd.Series(expected_result))


SUFFIX_EQUAL_TO_TEST_DATA = [
    (
        {"target": ["Atest", "B", "Ctest"], "VAR2": ["test", "test", "test"]},
        "target",
        "VAR2",
        False,
        4,
        [True, False, True],
        None,
    ),
    (
        {"target": ["A", "B", "C"], "VAR2": ["A", "B", "C"]},
        "target",
        "VAR2",
        False,
        1,
        [True, True, True],
        None,
    ),
    (
        {"target": ["Atest", "Bdemo", "Ctest"]},
        "target",
        ["test", "demo"],
        True,
        4,
        [True, True, True],
        None,
    ),
    (
        {"target": ["Atest", "B", "C"]},
        "target",
        "test",
        True,
        4,
        [True, False, False],
        None,
    ),
    (
        {"target": ["Ctt", "Btt", "A"]},
        "target",
        "$constant",
        False,
        1,
        [False, False, True],
        None,
    ),
    (
        {"target": ["AAA", "BAB", "CAC"]},
        "target",
        "$list",
        False,
        1,
        [True, True, False],
        None,
    ),
    (
        {"target": ["Atest", "B", None], "VAR2": ["test", "test", "test"]},
        "target",
        "VAR2",
        False,
        2,
        [False, False, False],
        None,
    ),
    (
        {"target": ["Atest", "", "Ctest"], "VAR2": ["", "", ""]},
        "target",
        "VAR2",
        False,
        4,
        [False, False, False],
        None,
    ),
    (
        {"target": ["Atest", "B", "Ctest"]},
        "target",
        "$constant",
        False,
        4,
        [False, False, False],
        None,
    ),
    # Dataset name test cases
    (
        {"dummy_col": ["val1", "val2", "val3"]},
        "dataset_name",
        "DATA",
        True,
        4,
        [True, True, True],
        "TESTDATA",
    ),
    (
        {"dummy_col": ["val1", "val2"]},
        "dataset_name",
        "DATA",
        True,
        3,
        [False, False],
        "TESTDATA",
    ),
]


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,length,expected_result,dataset_name",
    SUFFIX_EQUAL_TO_TEST_DATA,
)
def test_suffix_equal_to(data, target, comparator, value_is_literal, length, expected_result, dataset_name):
    sql_ops = create_sql_operators(data, dataset_name=dataset_name)

    result = sql_ops.suffix_equal_to(
        {
            "target": target,
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,length,expected_result,dataset_name",
    SUFFIX_EQUAL_TO_TEST_DATA,
)
def test_suffix_not_equal_to(data, target, comparator, value_is_literal, length, expected_result, dataset_name):
    sql_ops = create_sql_operators(data, dataset_name=dataset_name)

    result = sql_ops.suffix_not_equal_to(
        {
            "target": target,
            "comparator": comparator,
            "suffix": length,
            "value_is_literal": value_is_literal,
        }
    )

    assert_series_equals(result, ~pd.Series(expected_result))
