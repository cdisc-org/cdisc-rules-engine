import pytest

from .helpers import (
    assert_operation_constant,
    assert_operation_parameterized_constant,
    setup_sql_operations,
)


@pytest.mark.parametrize(
    "data, op, expected",
    [
        ({"values": [11, 12, 12, 5, 18, 9]}, "max", 18),
        ({"values": [11, 12, 12, 5, 18, 9]}, "min", 5),
        ({"values": [11, 12, 12, 5, 17, 9]}, "mean", 11),
    ],
)
def test_sql_maximum(data, op, expected):
    operation = setup_sql_operations(op, "values", data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, op, expected",
    [
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 18, 9]},
            "max",
            [
                {"params": {"$1": 1}, "value": [12.0]},
                {"params": {"$1": 2}, "value": [18.0]},
                {"params": {"$1": 3}, "value": [9.0]},
            ],
        ),
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 18, 9]},
            "min",
            [
                {"params": {"$1": 1}, "value": [11.0]},
                {"params": {"$1": 2}, "value": [5.0]},
                {"params": {"$1": 3}, "value": [9.0]},
            ],
        ),
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 13, 4, 18, 9]},
            "mean",
            [
                {"params": {"$1": 1}, "value": [12.0]},
                {"params": {"$1": 2}, "value": [11.0]},
                {"params": {"$1": 3}, "value": [9.0]},
            ],
        ),
    ],
)
def test_sql_maximum_grouping(data, op, expected):
    operation = setup_sql_operations(op, "values", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_parameterized_constant(operation, result, expected)
