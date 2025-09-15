import pandas as pd
import pytest

from .helpers import assert_series_equals, create_sql_operators


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
        (
            {"target": ["2023-01-15", "2025-09-09", "2023-03-10"]},
            "$date",
            False,
            [False, True, False],
        ),
        (
            {"target": ["2023-01-15", "2025-09", "2023-03-10"]},
            "2023-01-15",
            True,
            [True, False, False],
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
    assert_series_equals(result_incomplete, ~pd.Series(expected_incomplete))


@pytest.mark.parametrize(
    "data,expected_result",
    [
        (
            {"target": ["2021", "2099", "2022", "2023"]},
            [False, False, False, False],
        ),
        (
            {"target": ["90999", "20999", "2022", "2023"]},
            [True, True, False, False],
        ),
        (
            {
                "target": [
                    "2022-03-11T092030",
                    "2022-03-11T09,20,30",
                    "2022-03-11T09@20@30",
                    "2022-03-11T09!20:30",
                ]
            },
            [True, True, True, True],
        ),
        (
            {
                "target": [
                    "1997-07",
                    "1997-07-16",
                    "1997-07-16T19:20:30.45+01:00",
                    "2022-05-08T13:44:66",
                ]
            },
            [False, False, False, True],
        ),
        (
            {
                "target": [
                    "1",
                    "9999",
                    "10000",
                    "-1",
                ]
            },
            [True, False, True, True],
        ),
        (
            {
                "target": [
                    "2023-00",
                    "2023-01",
                    "2023-12",
                    "2023-13",
                ]
            },
            [True, False, False, True],
        ),
        (
            {
                "target": [
                    "2023-",
                    "2023-05-",
                    "2023--",
                    "2023-05--",
                ]
            },
            [True, True, False, False],
        ),
        (
            {
                "target": [
                    "2023-02-29",
                    "2024-02-29",
                    "2023-04-31",
                    "2023-02-00",
                ]
            },
            [True, False, True, True],
        ),
        (
            {
                "target": [
                    "2023-01-01T23:59:59",
                    "2023-01-01T24:00:00",
                    "2023-01-01T12:60:00",
                    "2023-01-01T12:30:60",
                ]
            },
            [False, True, True, True],
        ),
        (
            {
                "target": [
                    "2023-01-01T12:30:45Z",
                    "2023-01-01T12:30:45+05:30",
                    "2023-01-01T12:30:45.123",
                    "2023-01-01T12:30:45.999Z",
                ]
            },
            [False, False, False, False],
        ),
        (
            {
                "target": [
                    "not-a-date",
                    "2023/01/01",
                    "01-01-2023",
                    "2023.01.01",
                ]
            },
            [True, True, True, True],
        ),
        (
            {
                "target": [
                    "",
                    None,
                    " ",
                    "2023 01 01",
                    "2023-01-01 ",
                ]
            },
            [True, True, True, True, True],
        ),
        (
            {
                "target": [
                    "2023-01-01T00:00:00",
                    "2023-01-01T23:59:59.999",
                    "2023-01-01T25:00:00",
                    "2023-01-01T12:30",
                ]
            },
            [False, False, True, False],
        ),
        (
            {
                "target": [
                    "0001",
                    "2023-01",
                    "2023-1",
                    "23-01-01",
                ]
            },
            [False, False, True, True],
        ),
        (
            {
                "target": [
                    "2023-01/2023-02",
                    "2023-01-01T12:-:",
                    "2023-01-01/2023-01-02T12:30:45",
                    "2023-01-01T12:30:-",
                ]
            },
            [False, True, False, True],
        ),
        (
            {
                "target": [
                    "2023-01/invalid",
                    "2023-01-01T25:-:",
                    "invalid/2023-02",
                    "2023-01-01T12:65:-",
                ]
            },
            [True, True, True, True],
        ),
    ],
)
def test_invalid_date_sql(data, expected_result):
    sql_ops = create_sql_operators(data)
    result = sql_ops.invalid_date({"target": "target"})
    assert_series_equals(result, expected_result)
