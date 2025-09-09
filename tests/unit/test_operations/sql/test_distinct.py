import pytest

from .helpers import (
    assert_operation_list,
    assert_operation_table,
    setup_sql_operations,
)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {"values": [11, 12, 12, 5, 18, 9]},
            {5, 9, 11, 12, 18},
        ),
    ],
)
def test_distinct(data, expected):
    operation = setup_sql_operations("distinct", "values", data)
    result = operation.execute()
    assert_operation_list(operation, result, expected, unsorted=True)


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {"grp": [1, 1, 1, 2, 2, 3], "values": [11, 12, 12, 5, 18, 9]},
            [
                # Not sure whether we're going to stick with this structure in the end, but doing it like this for now
                {"grp": 1, "value": 11},
                {"grp": 1, "value": 12},
                {"grp": 2, "value": 5},
                {"grp": 2, "value": 18},
                {"grp": 3, "value": 9},
            ],
        ),
    ],
)
def test_sql_maximum_grouping(data, expected):
    operation = setup_sql_operations("distinct", "values", data, extra_config={"grouping": ["grp"]})
    result = operation.execute()
    assert_operation_table(operation, result, expected)
