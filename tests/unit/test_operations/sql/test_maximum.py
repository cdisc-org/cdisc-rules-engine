import pytest

from .helpers import (
    assert_operation_constant,
    assert_operation_table,
    setup_sql_operations,
)


@pytest.mark.parametrize(
    "data, expected",
    [
        ({"values": [11, 12, 12, 5, 18, 9]}, 18),
    ],
)
def test_sql_maximum(data, expected):
    operation = setup_sql_operations("max", "values", data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 18, 9]},
            [
                {"grp": 1, "value": 12},
                {"grp": 2, "value": 18},
                {"grp": 3, "value": 9},
            ],
        ),
    ],
)
def test_sql_maximum_grouping(data, expected):
    operation = setup_sql_operations("max", "values", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_table(operation, result, expected)
