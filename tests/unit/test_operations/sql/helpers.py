from typing import Any, List

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

TEST_TABLE_NAME = "test_table"


def setup_sql_operations(
    operation: str, target: str, column_data: dict, extra_operation_variables: dict = {}, extra_config: dict = {}
):
    """Create PostgresQLOperators instance with test data.

    Args:
        operation: The SQL operation to be performed
        target: The target column for the operation
        column_data: Dictionary containing column names and their data
        extra_operation_variables: Optional additional custom dictionary of operation variables
        extra_config: Optional additional configuration for the operations

    Returns:
        PostgresQLOperators instance configured for testing
    """
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(data_service, table_name=TEST_TABLE_NAME, column_data=column_data)

    params = SqlOperationParams(
        domain=TEST_TABLE_NAME,
        target=target,
        # TODO: When relevant
        standard="",
        standard_version="",
        **extra_config,
    )
    return SqlOperationsFactory.get_service(operation, params, data_service)


def assert_operation_constant(operation: SqlBaseOperation, result: SqlOperationResult, expected: Any):
    """Assert that the result of an operation is a constant value."""
    assert isinstance(result, SqlOperationResult)
    assert result.type == "constant"

    operation.data_service.pgi.execute_sql(result.query)
    rows = operation.data_service.pgi.fetch_all()
    assert len(rows) == 1
    row = rows[0]
    assert "value" in row, "The result column must be called 'value'"
    assert row["value"] == expected


def assert_operation_list(
    operation: SqlBaseOperation, result: SqlOperationResult, expected: List[Any], unsorted: bool = False
):
    """Assert that the result of an operation is a list value."""
    assert isinstance(result, SqlOperationResult)
    assert result.type == "collection"

    operation.data_service.pgi.execute_sql(result.query)
    rows = operation.data_service.pgi.fetch_all()
    assert len(rows) == len(expected), f"Expected {len(expected)} rows, got {len(rows)}"

    for row in rows:
        assert "value" in row, "The result column must be called 'value'"

    if unsorted:
        rows = sorted([row["value"] for row in rows])
        expected = sorted(expected)

    for i, row in enumerate(rows):
        assert row == expected[i], f"Row {i} does not match expected data: {row} != {expected[i]}"


def assert_operation_table(operation: SqlBaseOperation, result: SqlOperationResult, expected: List[dict[str, Any]]):
    """Assert that the result of an operation is a table value."""
    assert isinstance(result, SqlOperationResult)
    assert result.type == "table"

    operation.data_service.pgi.execute_sql(result.query)
    rows = operation.data_service.pgi.fetch_all()
    assert len(rows) == len(expected)
    for i, row in enumerate(rows):
        assert row == expected[i], f"Row {i} does not match expected data: {row} != {expected[i]}"
