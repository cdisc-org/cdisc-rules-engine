import pandas as pd
import pytest

from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from .helpers import create_sql_operators, assert_series_equals

CONTAINS_TEST_DATA = [
    (
        {"target": ["LBSEQ", "AESEQ", "A"], "VAR2": ["LB", "AE", "A"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["TOXGR", "GRADE", "LBTEST"]},
        "GR",
        True,
        [True, True, False],
    ),
    (
        {"target": ["LBSEQ", "AESEQ", "DMSEQ"], "VAR2": ["XY", "ZZ", "AA"]},
        "VAR2",
        False,
        [False, False, False],
    ),
    (
        {"target": ["LBTEST", "AETERM", "DOMAIN"]},
        ["LB", "AE", "XY"],
        True,
        [True, True, False],
    ),
    (
        {"target": ["LBTEST", "AETEST", "DMTEST"], "VAR2": ["TEST", "TEST", "TEST"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["ABC", "XYZ", "A123"]},
        "$constant",
        False,
        [True, False, True],
    ),
    (
        {"target": ["B", "c", "a"]},
        "$list",
        False,
        [True, False, False],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINS_TEST_DATA,
)
def test_sql_contains(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.contains(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINS_TEST_DATA,
)
def test_sql_does_not_contain(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.does_not_contain(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, ~pd.Series(expected_result))


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


CONTAINS_CASE_INSENSITIVE_TEST_DATA = [
    (
        {"target": ["LBseq", "AEseq", "A"], "VAR2": ["lb", "AE", "a"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["TOXGR", "grade", "LBTEST"]},
        "gr",
        True,
        [True, True, False],
    ),
    (
        {"target": ["LBTEST", "aeterm", "DOMAIN"]},
        ["lb", "AE", "xy"],
        True,
        [True, True, False],
    ),
    (
        {"target": ["LBTest", "AETest", "DMTest"], "VAR2": ["TEST", "test", "Test"]},
        "VAR2",
        False,
        [True, True, True],
    ),
    (
        {"target": ["abc", "XYZ", "A123"]},
        "$constant",
        False,
        [True, False, True],
    ),
    (
        {"target": ["b", "C", "ab"]},
        "$list",
        False,
        [True, False, True],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINS_CASE_INSENSITIVE_TEST_DATA,
)
def test_sql_contains_case_insensitive(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.contains_case_insensitive(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINS_CASE_INSENSITIVE_TEST_DATA,
)
def test_sql_does_not_contain_case_insensitive(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.does_not_contain_case_insensitive(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
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


CONTAINS_ALL_TEST_DATA = [
    (
        {"target": ["Ctt", "Btt", "A"], "VAR2": ["A", "Btt", "A"]},
        "VAR2",
        False,
        True,
    ),
    (
        {"target": ["A", "B", "C", "D"]},
        ["A", "B", "C"],
        True,
        True,
    ),
    (
        {"target": ["A", "B", "C"]},
        [],
        True,
        True,
    ),
    (
        {"target": ["A", "B", "C"]},
        ["B"],
        True,
        True,
    ),
    (
        {"target": ["A", "B", "C"]},
        "$constant",
        False,
        True,
    ),
    (
        {"target": ["A", "B", "C", "D"]},
        "$list",
        False,
        True,
    ),
    # Negative test cases (should return False)
    (
        {"target": ["A", "B", "D"], "VAR2": ["A", "B", "C"]},
        "VAR2",
        False,
        False,
    ),
    (
        {"target": ["X", "Y", "Z"]},
        ["A", "B"],
        True,
        False,
    ),
    (
        {"target": ["A", "B", "C"]},
        ["A", "B", "D"],
        True,
        False,
    ),
    (
        {"target": ["A", "B"]},
        ["A", "B", "C"],
        True,
        False,
    ),
    (
        {"target": ["B"]},
        [""],
        True,
        False,
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINS_ALL_TEST_DATA,
)
def test_sql_contains_all(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.contains_all(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    expected_series = [expected_result] * len(data["target"])
    assert_series_equals(result, expected_series)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    CONTAINS_ALL_TEST_DATA,
)
def test_sql_not_contains_all(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.not_contains_all(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    expected_series = [expected_result] * len(data["target"])
    assert_series_equals(result, ~pd.Series(expected_series))


IS_SUBSTRING_OF_TEST_DATA = [
    (
        {"target": ["QT", "ECG", "SYS"]},
        "QT Studies",
        True,
        [True, False, False],
    ),
    (
        {"target": ["B12", "C", "A"]},
        ["Vitamin B12", "Vitamin A", "Zinc"],
        True,
        [True, False, True],
    ),
    (
        {"target": ["LB", "AE", "DM"], "VAR2": ["LBCAT", "AETERM", "XX"]},
        "VAR2",
        False,
        [True, True, False],
    ),
    (
        {"target": ["A", "B", "C"]},
        "$constant",
        False,
        [True, False, False],
    ),
    (
        {"target": ["B", "c", "a"]},
        "$list",
        False,
        [True, False, False],
    ),
    (
        {"target": ["", None, "A"]},
        "Vitamin A",
        True,
        [False, False, True],
    ),
]


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    IS_SUBSTRING_OF_TEST_DATA,
)
def test_is_substring_of(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_substring_of(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    IS_SUBSTRING_OF_TEST_DATA,
)
def test_is_not_substring_of(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_not_substring_of(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, ~pd.Series(expected_result))


def test_sql_contains_all_collection_target_collection_comparator():
    """Test contains_all when both target and comparator are collection operation variables."""
    extra_vars = {
        "$collection_abc": SqlOperationResult(
            query="SELECT value FROM (VALUES ('A'), ('B'), ('C')) as t(value)",
            type="collection",
            subtype="Char",
        )
    }
    # One-row table so the global result is returned once
    sql_ops = create_sql_operators({"dummy": ["X"]}, extra_operation_variables=extra_vars)

    # $collection_abc = [A, B, C] contains all of $list = [A, B] => True
    result = sql_ops.contains_all({"target": "$collection_abc", "comparator": "$list", "value_is_literal": False})
    assert_series_equals(result, [True])

    # $list = [A, B] does NOT contain all of $collection_abc = [A, B, C] => False
    result = sql_ops.contains_all({"target": "$list", "comparator": "$collection_abc", "value_is_literal": False})
    assert_series_equals(result, [False])


def test_sql_contains_all_collection_target_list_comparator():
    """Test contains_all when target is a collection variable and comparator is a plain list."""
    extra_vars = {
        "$collection_abc": SqlOperationResult(
            query="SELECT value FROM (VALUES ('A'), ('B'), ('C')) as t(value)",
            type="collection",
            subtype="Char",
        )
    }
    sql_ops = create_sql_operators({"dummy": ["X"]}, extra_operation_variables=extra_vars)

    # $collection_abc = [A, B, C] contains all of [A, B] => True
    result = sql_ops.contains_all({"target": "$collection_abc", "comparator": ["A", "B"], "value_is_literal": True})
    assert_series_equals(result, [True])

    # $collection_abc = [A, B, C] does NOT contain all of [A, B, D] => False
    result = sql_ops.contains_all(
        {"target": "$collection_abc", "comparator": ["A", "B", "D"], "value_is_literal": True}
    )
    assert_series_equals(result, [False])
