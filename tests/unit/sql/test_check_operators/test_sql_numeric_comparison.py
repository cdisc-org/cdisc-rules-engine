import pytest

from .helpers import assert_series_equals, create_sql_operators


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            [True, True, False],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            2,
            [True, False, False],
        ),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            [True, True, False],
        ),
        (
            {"target": ["1", "2", "3"]},
            "$number",
            [False, False, False],
        ),
        (
            {"target": ["1", "2", "3"]},
            None,
            [False, False, False],
        ),
        (
            {"target": ["1", None, "3"]},
            1,
            [False, False, False],
        ),
    ],
)
def test_sql_less_than(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.less_than({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            [True, True, True],
        ),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, [True, True, False]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            [True, True, True],
        ),
        (
            {"target": ["1", "2", "3"]},
            "$number",
            [True, False, False],
        ),
        (
            {"target": ["1", "2", "3"]},
            None,
            [False, False, False],
        ),
        (
            {"target": ["1", None, "3"]},
            1,
            [True, False, False],
        ),
    ],
)
def test_sql_less_than_or_equal_to(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.less_than_or_equal_to({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": [1, 2, 4], "VAR2": [3, 3, 3]},
            "VAR2",
            [False, False, True],
        ),
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            2,
            [False, False, True],
        ),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            [False, False, False],
        ),
        (
            {"target": ["1", "2", "3"]},
            "$number",
            [False, True, True],
        ),
        (
            {"target": ["1", "2", "3"]},
            None,
            [False, False, False],
        ),
        (
            {"target": ["1", None, "3"]},
            1,
            [False, False, True],
        ),
    ],
)
def test_sql_greater_than(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.greater_than({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,expected_result",
    [
        (
            {"target": [1, 2, 3], "VAR2": [3, 3, 3]},
            "VAR2",
            [False, False, True],
        ),
        ({"target": [1, 2, 3], "VAR2": [3, 3, 3]}, 2, [False, True, True]),
        (
            {"target": ["1", "2", "3"], "VAR2": ["3", "3", "3"]},
            "VAR2",
            [False, False, True],
        ),
        (
            {"target": ["1", "2", "3"]},
            "$number",
            [True, True, True],
        ),
        (
            {"target": ["1", "2", "3"]},
            None,
            [False, False, False],
        ),
        (
            {"target": ["1", None, "3"]},
            1,
            [True, False, True],
        ),
    ],
)
def test_sql_greater_than_or_equal_to(data, comparator, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.greater_than_or_equal_to({"target": "target", "comparator": comparator})
    assert_series_equals(result, expected_result)
