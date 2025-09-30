import pytest
from .helpers import assert_series_equals, create_sql_operators

is_ordered_set_test_data = [
    (
        {"target": ["1", "2", "3"], "groupby": ["A", "A", "A"]},
        "target",
        "groupby",
        False,
        [True, True, True],
    ),
    (
        {"target": ["3", "2", "1"], "groupby": ["A", "A", "A"]},
        "target",
        "groupby",
        False,
        [False, False, False],
    ),
    (
        {"target": ["1", "2", "1", "3"], "groupby": ["A", "A", "B", "B"]},
        "target",
        "groupby",
        False,
        [True, True, True, True],
    ),
    (
        {"target": ["3", "1", "1", "2"], "groupby": ["A", "A", "B", "B"]},
        "target",
        "groupby",
        False,
        [False, False, True, True],
    ),
    (
        {"target": ["A", "B", "C"], "groupby": ["X", "Y", "Z"]},
        "target",
        "groupby",
        False,
        [True, True, True],
    ),
    (
        {"target": ["1", "1", "2", "2"], "groupby": ["A", "A", "B", "B"]},
        "target",
        "groupby",
        False,
        [True, True, True, True],
    ),
    (
        {"target": ["1", "3", "2", "4", "5", "6"], "groupby": ["A", "A", "B", "B", "C", "C"]},
        "target",
        "groupby",
        False,
        [True, True, True, True, True, True],
    ),
    (
        {"target": ["1", "3", "4", "2", "5", "6"], "groupby": ["A", "A", "B", "B", "C", "C"]},
        "target",
        "groupby",
        False,
        [True, True, False, False, True, True],
    ),
]

is_not_ordered_set_test_data = [
    (
        {"target": ["1", "2", "3"], "groupby": ["A", "A", "A"]},
        "target",
        "groupby",
        False,
        [False, False, False],
    ),
    (
        {"target": ["3", "2", "1"], "groupby": ["A", "A", "A"]},
        "target",
        "groupby",
        False,
        [True, True, True],
    ),
    (
        {"target": ["1", "2", "1", "3"], "groupby": ["A", "A", "B", "B"]},
        "target",
        "groupby",
        False,
        [False, False, False, False],
    ),
    (
        {"target": ["3", "1", "1", "2"], "groupby": ["A", "A", "B", "B"]},
        "target",
        "groupby",
        False,
        [True, True, False, False],
    ),
    (
        {"target": ["A", "B", "C"], "groupby": ["X", "Y", "Z"]},
        "target",
        "groupby",
        False,
        [False, False, False],
    ),
    (
        {"target": ["1", "1", "2", "2"], "groupby": ["A", "A", "B", "B"]},
        "target",
        "groupby",
        False,
        [False, False, False, False],
    ),
]


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,expected_result",
    is_ordered_set_test_data,
)
def test_is_ordered_set(data, target, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_ordered_set(
        {
            "target": target,
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,target,comparator,value_is_literal,expected_result",
    is_not_ordered_set_test_data,
)
def test_is_not_ordered_set(data, target, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.is_not_ordered_set(
        {
            "target": target,
            "comparator": comparator,
            "value_is_literal": value_is_literal,
        }
    )
    assert_series_equals(result, expected_result)
