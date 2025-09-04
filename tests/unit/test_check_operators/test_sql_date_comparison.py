import pandas as pd
import pytest

from .helpers import create_sql_operators, assert_series_equals


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-15", "2023-02-25", "2023-03-10"]},
            "VAR2",
            False,
            [True, False, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-05"]},
            "2023-02-20",
            True,
            [False, True, False],
        ),
    ],
)
def test_sql_date_equal_to(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_equal_to({"target": "target", "comparator": comparator, "value_is_literal": value_is_literal})
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-15", "2023-02-25", "2023-03-10"]},
            "VAR2",
            False,
            [False, True, False],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-05"]},
            "2023-02-20",
            True,
            [True, False, True],
        ),
    ],
)
def test_sql_date_not_equal_to(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_not_equal_to(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-15"]},
            "VAR2",
            False,
            [True, False, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-10", "2023-02-25", "2023-03-05"]},
            "2023-02-01",
            True,
            [True, False, False],
        ),
    ],
)
def test_sql_date_less_than(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_less_than(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-20", "2023-03-15"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-10", "2023-02-25", "2023-03-05"]},
            "2023-02-20",
            True,
            [True, True, False],
        ),
    ],
)
def test_sql_date_less_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_less_than_or_equal_to(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-10", "2023-02-25", "2023-03-05"]},
            "VAR2",
            False,
            [True, False, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-15"]},
            "2023-02-01",
            True,
            [False, True, True],
        ),
    ],
)
def test_sql_date_greater_than(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_greater_than(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-10", "2023-02-20", "2023-03-05"]},
            "VAR2",
            False,
            [True, True, True],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-15", "2023-03-15"]},
            "2023-02-20",
            True,
            [False, True, True],
        ),
    ],
)
def test_sql_date_greater_than_or_equal_to(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_greater_than_or_equal_to(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-15", "2023-02-20", "2023-03-10"]},
            "VAR2",
            False,
            [False, False, False],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-20", "2023-03-05"]},
            "VAR2",
            False,
            [True, False, False],
        ),
        (
            {"target": ["2023-02-20", "2023-02-19", "2023-02-21"]},
            "2023-02-20",
            True,
            [False, True, False],
        ),
    ],
)
def test_sql_date_less_than_equality_boundary(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_less_than(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-15", "2023-02-20", "2023-03-10"]},
            "VAR2",
            False,
            [False, False, False],
        ),
        (
            {"target": ["2023-01-15", "2023-02-20", "2023-03-10"], "VAR2": ["2023-01-20", "2023-02-20", "2023-03-05"]},
            "VAR2",
            False,
            [False, False, True],
        ),
        (
            {"target": ["2023-02-20", "2023-02-19", "2023-02-21"]},
            "2023-02-20",
            True,
            [False, False, True],
        ),
    ],
)
def test_sql_date_greater_than_equality_boundary(data, comparator, value_is_literal, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_greater_than(
        {"target": "target", "comparator": comparator, "value_is_literal": value_is_literal}
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,date_component,expected_result",
    [
        (
            {"target": ["2023-01-15", "2023-06-20", "2024-03-10"], "VAR2": ["2023-12-25", "2023-01-01", "2024-12-31"]},
            "VAR2",
            False,
            "year",
            [True, True, True],
        ),
        (
            {"target": ["2023-01-15", "2024-06-20", "2025-03-10"]},
            "2023-12-31",
            True,
            "year",
            [True, False, False],
        ),
        (
            {"target": ["2023-01-15", "2024-01-20", "2023-02-10"], "VAR2": ["2022-01-25", "2025-01-01", "2024-02-28"]},
            "VAR2",
            False,
            "month",
            [True, True, True],
        ),
        (
            {"target": ["2023-01-15", "2024-06-15", "2023-02-20"], "VAR2": ["2022-12-15", "2025-03-15", "2024-07-20"]},
            "VAR2",
            False,
            "day",
            [True, True, True],
        ),
    ],
)
def test_sql_date_equal_to_with_components(data, comparator, value_is_literal, date_component, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_equal_to(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
            "date_component": date_component,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,date_component,expected_result",
    [
        (
            {"target": ["2023-12-31", "2024-01-01", "2025-06-15"]},
            "2024-01-01",
            True,
            "year",
            [True, False, False],
        ),
        (
            {"target": ["2023-01-31", "2023-03-15", "2023-05-10"], "VAR2": ["2023-02-01", "2023-02-28", "2023-02-15"]},
            "VAR2",
            False,
            "month",
            [True, False, False],
        ),
        (
            {"target": ["2023-01-10", "2023-01-15", "2023-01-20"], "VAR2": ["2023-01-15", "2023-01-15", "2023-01-15"]},
            "VAR2",
            False,
            "day",
            [True, False, False],
        ),
    ],
)
def test_sql_date_less_than_with_components(data, comparator, value_is_literal, date_component, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_less_than(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
            "date_component": date_component,
        }
    )
    assert_series_equals(result, expected_result)


@pytest.mark.parametrize(
    "data,comparator,value_is_literal,date_component,expected_result",
    [
        (
            {"target": ["2023-01-15T09:30:00", "2023-01-15T14:45:00", "2023-01-15T20:15:00"]},
            "2023-01-15T12:00:00",
            True,
            "hour",
            [True, False, False],
        ),
        (
            {"target": ["2023-01-15T12:15:30", "2023-01-15T12:30:45", "2023-01-15T12:45:00"]},
            "2023-01-15T12:30:00",
            True,
            "minute",
            [True, True, False],
        ),
        (
            {"target": ["2023-01-15T12:30:15", "2023-01-15T12:30:30", "2023-01-15T12:30:45"]},
            "2023-01-15T12:30:30",
            True,
            "second",
            [True, True, False],
        ),
    ],
)
def test_sql_date_time_components(data, comparator, value_is_literal, date_component, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.date_less_than_or_equal_to(
        {
            "target": "target",
            "comparator": comparator,
            "value_is_literal": value_is_literal,
            "date_component": date_component,
        }
    )
    assert_series_equals(result, expected_result)


IS_COMPLETE_DATE_TEST_DATA = [
    (
        {
            "target": [
                "2020-01-01",
                "2020-01",
                "2020",
                "08/04",
                "1987-30",
                "2020-01-01T17:00:00+01:00",
                "2020-01-01T19:20+01:00",
            ]
        },
        [True, False, False, False, False, True, True],
    )
]


@pytest.mark.parametrize(
    "data,expected_complete",
    IS_COMPLETE_DATE_TEST_DATA,
)
def test_is_complete_date_sql(data, expected_complete):
    sql_ops = create_sql_operators(data)
    result_complete = sql_ops.is_complete_date({"target": "target"})
    assert_series_equals(result_complete, expected_complete)


@pytest.mark.parametrize(
    "data,expected_incomplete",
    IS_COMPLETE_DATE_TEST_DATA,
)
def test_is_incomplete_date_sql(data, expected_incomplete):
    sql_ops = create_sql_operators(data)
    result_incomplete = sql_ops.is_incomplete_date({"target": "target"})
    assert_series_equals(result_incomplete, (~pd.Series(expected_incomplete)).tolist())
