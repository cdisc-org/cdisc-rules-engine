from .helpers import (
    assert_operation_constant,
    assert_operation_parameterized_constant,
    setup_sql_operations,
)
import pytest


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"dates": ["2001-01-01", "2022-01-05", "2010-12-12"]}, "2022-01-05"),
        ({"dates": [None, None]}, ""),
        ({"dates": ["1999-12-31", "2000-01-01", "1999-01-01"]}, "2000-01-01"),
        ({"dates": ["2023-06-15"]}, "2023-06-15"),
    ],
)
def test_max_date(data, expected):
    operation = setup_sql_operations("max_date", "dates", data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {"grp": [1, 1, 2, 2], "dates": ["2001-01-01", "2022-01-05", "2010-12-12", "2023-01-01"]},
            [
                {"params": {"$1": 1}, "value": ["2022-01-05"]},
                {"params": {"$1": 2}, "value": ["2023-01-01"]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": ["2001-01-01", None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": ["2001-01-01"]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": [None, None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": [""]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 2, 3], "dates": ["2020-01-01", "2021-12-31", "2019-06-15"]},
            [
                {"params": {"$1": 1}, "value": ["2020-01-01"]},
                {"params": {"$1": 2}, "value": ["2021-12-31"]},
                {"params": {"$1": 3}, "value": ["2019-06-15"]},
            ],
        ),
    ],
)
def test_max_date_grouping(data, expected):
    operation = setup_sql_operations("max_date", "dates", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_parameterized_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"dates": ["2001-01-01", "2022-01-05", "2010-12-12"]}, "2001-01-01"),
        ({"dates": [None, None]}, ""),
        ({"dates": ["1999-12-31", "2000-01-01", "1999-01-01"]}, "1999-01-01"),
        ({"dates": ["2023-06-15"]}, "2023-06-15"),
    ],
)
def test_min_date(data, expected):
    operation = setup_sql_operations("min_date", "dates", data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {"grp": [1, 1, 2, 2], "dates": ["2001-01-01", "2022-01-05", "2010-12-12", "2023-01-01"]},
            [
                {"params": {"$1": 1}, "value": ["2001-01-01"]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": ["2001-01-01", None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": ["2001-01-01"]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 1, 2], "dates": [None, None, "2010-12-12"]},
            [
                {"params": {"$1": 1}, "value": [""]},
                {"params": {"$1": 2}, "value": ["2010-12-12"]},
            ],
        ),
        (
            {"grp": [1, 2, 3], "dates": ["2020-01-01", "2021-12-31", "2019-06-15"]},
            [
                {"params": {"$1": 1}, "value": ["2020-01-01"]},
                {"params": {"$1": 2}, "value": ["2021-12-31"]},
                {"params": {"$1": 3}, "value": ["2019-06-15"]},
            ],
        ),
    ],
)
def test_min_date_grouping(data, expected):
    operation = setup_sql_operations("min_date", "dates", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_parameterized_constant(operation, result, expected)
