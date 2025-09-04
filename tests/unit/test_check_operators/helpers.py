"""Helper functions for SQL operator tests."""

import pandas as pd

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)

TEST_TABLE_NAME = "test_table"


def create_sql_operators(column_data: dict, operation_variables: dict = None) -> PostgresQLOperators:
    """Create PostgresQLOperators instance with test data.

    Args:
        column_data: Dictionary containing column names and their data
        operation_variables: Optional dictionary of operation variables

    Returns:
        PostgresQLOperators instance configured for testing
    """
    data_service = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name=TEST_TABLE_NAME, column_data=column_data)

    config = {"validation_dataset_id": TEST_TABLE_NAME, "sql_data_service": data_service}

    if operation_variables:
        config["operation_variables"] = operation_variables

    return PostgresQLOperators(config)


def create_sql_operators_with_config(column_data: dict, extra_config: dict = None) -> PostgresQLOperators:
    """Create PostgresQLOperators instance with test data and additional configuration.

    Args:
        column_data: Dictionary containing column names and their data
        extra_config: Additional configuration parameters

    Returns:
        PostgresQLOperators instance configured for testing
    """
    data_service = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name=TEST_TABLE_NAME, column_data=column_data)

    config = {"validation_dataset_id": TEST_TABLE_NAME, "sql_data_service": data_service}

    if extra_config:
        config.update(extra_config)

    return PostgresQLOperators(config)


def assert_series_equals(actual: pd.Series, expected):
    """Assert that pandas Series equals expected values.

    Args:
        actual: The actual pandas Series result
        expected: Expected list of values or pandas Series
    """
    if isinstance(expected, pd.Series):
        expected_series = expected
    else:
        expected_series = pd.Series(expected)

    if not actual.equals(expected_series):
        failing_rows = []
        for i in range(min(len(actual), len(expected_series))):
            if actual.iloc[i] != expected_series.iloc[i]:
                failing_rows.append(f"Row {i}: expected {expected_series.iloc[i]}, got {actual.iloc[i]}")

        if len(actual) != len(expected_series):
            failing_rows.append(f"Length mismatch: expected {len(expected_series)}, got {len(actual)}")

        error_msg = "\nAssertion failed\n"
        error_msg += "Failing rows:\n"
        for row_info in failing_rows:
            error_msg += f"  {row_info}\n"
        error_msg += f"Expected: {expected_series.tolist()}\n"
        error_msg += f"Actual:   {actual.tolist()}"

        assert False, error_msg

    assert actual.equals(expected_series)
