import pytest

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {"STUDYID": [1, 2, 3, 1, 2], "STUDYDESC": ["A", "B", "C", "A", "B"]},
            "STUDYID",
            "STUDYDESC",
            [False, False, False, False, False],
        ),
        (
            {"STUDYID": [1, 1, 2, 3], "STUDYDESC": ["A", "B", "B", "C"]},
            "STUDYID",
            "STUDYDESC",
            [True, True, True, False],
        ),
        (
            {"STUDYID": [1, 2, 3], "STUDYDESC": ["A", "A", "B"]},
            "STUDYID",
            "STUDYDESC",
            [True, True, False],
        ),
        (
            {"STUDYID": [1, 2, 3, 4], "STUDYDESC": ["A", "B", "C", "D"]},
            "STUDYID",
            "STUDYDESC",
            [False, False, False, False],
        ),
    ],
)
def test_sql_is_not_unique_relationship(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_not_unique_relationship({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {"STUDYID": [1, 2, 3, 1, 2], "STUDYDESC": ["A", "B", "C", "A", "B"]},
            "STUDYID",
            "STUDYDESC",
            [True, True, True, True, True],
        ),
        (
            {"STUDYID": [1, 1, 2, 3], "STUDYDESC": ["A", "B", "B", "C"]},
            "STUDYID",
            "STUDYDESC",
            [False, False, False, True],
        ),
        (
            {"STUDYID": [1, 2, 3], "STUDYDESC": ["A", "A", "B"]},
            "STUDYID",
            "STUDYDESC",
            [False, False, True],
        ),
    ],
)
def test_sql_is_unique_relationship(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_unique_relationship({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,expected_result",
    [
        (
            {"STUDYID": [1, 2, 3, 1, 2], "DOMAIN": ["AE", "DM", "LB", "AE", "DM"], "VISITNUM": [1, 2, 3, 1, 2]},
            "STUDYID",
            ["DOMAIN", "VISITNUM"],
            [False, False, False, False, False],
        ),
        (
            {"STUDYID": [1, 1, 2, 3], "DOMAIN": ["AE", "DM", "AE", "LB"], "VISITNUM": [1, 2, 1, 3]},
            "STUDYID",
            ["DOMAIN", "VISITNUM"],
            [True, True, True, False],
        ),
        (
            {"STUDYID": [1, 2, 3, 4], "DOMAIN": ["AE", "AE", "DM", "AE"], "VISITNUM": [1, 1, 2, 1]},
            "STUDYID",
            ["DOMAIN", "VISITNUM"],
            [True, True, False, True],
        ),
    ],
)
def test_sql_is_not_unique_relationship_multiple_comparators(data, target, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_not_unique_relationship({"target": target, "comparator": comparator})
    assert_series_equals(result, expected_result)
