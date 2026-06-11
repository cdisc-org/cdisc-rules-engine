import pytest

from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult

from .helpers import (
    assert_operation_collection,
    setup_sql_operations,
)

TEST_OPERATIONS = {
    "$op_name": SqlOperationResult(
        query="SELECT 'A' AS value UNION SELECT 'B' AS value", type="collection", subtype="Char"
    ),
    "$op_subtract": SqlOperationResult(
        query="SELECT 'B' AS value UNION SELECT 'C' AS value", type="collection", subtype="Char"
    ),
}


@pytest.mark.parametrize(
    "data, params, expected",
    [
        (
            {"col_name": ["A", "B", "C", "D"], "col_subtract": ["C", "D", "E", "F"]},
            {"name": "col_name", "subtract": "col_subtract"},
            ["A", "B"],
        ),
        (
            {"col_name": [1, 2, 3, 4], "col_subtract": [3, 4, 5, 6]},
            {"name": "col_name", "subtract": "col_subtract"},
            [1, 2],  # or ["1", "2"] depending on how the pg driver interprets the type in testing
        ),
        # Case 3: When the 'subtract' column has no matching values (all values should return)
        (
            {"col_name": ["X", "Y", "Z"], "col_subtract": [None, None, "W"]},
            {"name": "col_name", "subtract": "col_subtract"},
            ["X", "Y", "Z"],
        ),
        # Case 4: When the 'name' column is empty (should return empty collection)
        ({"col_name": [None, None], "col_subtract": ["A", "B"]}, {"name": "col_name", "subtract": "col_subtract"}, []),
        (
            {"dummy_col": ["1"]},  # table needs to be populated for setup_sql_operations to work
            {"name": "$op_name", "subtract": "$op_subtract"},
            ["A"],
        ),
    ],
)
def test_minus_operation(data, params, expected):
    """
    Tests the MINUS operation.
    """
    operation = setup_sql_operations(
        operation="minus",
        target=None,
        column_data=data,
        extra_config=params,
        extra_operation_variables=TEST_OPERATIONS,
    )

    result = operation.execute()
    assert_operation_collection(operation, result, expected, unsorted=True)
