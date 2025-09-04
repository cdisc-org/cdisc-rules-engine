import pytest

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data,expected_result",
    [
        (
            # {"target": ["Att", "", None, {None}, {None, 1}, {1, 2}]},
            # [False, True, True, True, False, False],
            {"target": ["Att", "", None]},
            [False, True, True],
        ),
        (
            {"target": [1, 2, None]},
            [False, False, True],
        ),
    ],
)
def test_empty(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.empty({"target": "target"})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,expected_result",
    [
        ({"target": ["Att", "", None]}, [True, False, False]),
        ({"target": [1, 2, None]}, [True, True, False]),
    ],
)
def test_non_empty(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.non_empty({"target": "target"})
    assert_series_equals(result, expected_result)
