import pytest

from .helpers import (
    assert_operation_constant,
    setup_sql_operations,
)


@pytest.mark.parametrize(
    "data, target, expected",
    [
        (
            {"existing_column": [1, 2, 3], "other_column": ["a", "b", "c"]},
            "existing_column",
            True,
        ),
        (
            {"existing_column": [1, 2, 3], "other_column": ["a", "b", "c"]},
            "other_column",
            True,
        ),
        (
            {"existing_column": [1, 2, 3], "other_column": ["a", "b", "c"]},
            "non_existing_column",
            False,
        ),
        (
            {"single_column": [1, 2, 3]},
            "single_column",
            True,
        ),
        (
            {"single_column": [1, 2, 3]},
            "missing_column",
            False,
        ),
    ],
)
def test_variable_exists(data, target, expected):
    operation = setup_sql_operations("variable_exists", target, data)
    result = operation.execute()
    assert_operation_constant(operation, result, expected)
