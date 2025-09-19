"""Helper functions for SQL operator tests."""

import pandas as pd
from datetime import datetime

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult

TEST_TABLE_NAME = "test_table"


def add_test_dataset_metadata(data_service: PostgresQLDataService, table_name: str, dataset_name: str = None):
    """Add test dataset metadata to the data_metadata table."""
    if dataset_name is None:
        dataset_name = table_name.upper()

    timestamp = datetime.now().astimezone()
    metadata_row = {
        "created_at": timestamp,
        "updated_at": timestamp,
        "dataset_filename": f"{table_name}.xpt",
        "dataset_filepath": f"/test/{table_name}.xpt",
        "dataset_id": table_name.lower(),
        "table_hash": table_name.lower(),
        "dataset_name": dataset_name,
        "dataset_label": f"Test {dataset_name} Dataset",
        "dataset_domain": None,
        "dataset_is_supp": False,
        "dataset_rdomain": None,
        "dataset_is_split": False,
        "dataset_unsplit_name": dataset_name,
        "dataset_preprocessed": None,
        "var_name": "dummy",
        "var_label": "Dummy Variable",
        "var_type": "text",
        "var_length": 200,
        "var_format": None,
    }

    data_service.pgi.insert_data(table_name="data_metadata", data=[metadata_row])


def create_sql_operators(
    column_data: dict, extra_operation_variables: dict = {}, extra_config: dict = {}, dataset_name: str = None
) -> PostgresQLOperators:
    """Create PostgresQLOperators instance with test data.
    It will preload some operation variables which can be used in tests.

    Args:
        column_data: Dictionary containing column names and their data
        extra_operation_variables: Optional additional custom dictionary of operation variables
        extra_config: Optional additional configuration for the operators
        dataset_name: Optional dataset name for testing dataset_name column operations

    Returns:
        PostgresQLOperators instance configured for testing
    """
    data_service = PostgresQLDataService.test_instance()
    PostgresQLDataService.add_test_dataset(data_service.pgi, table_name=TEST_TABLE_NAME, column_data=column_data)

    # Add dataset metadata if dataset_name is provided
    if dataset_name:
        add_test_dataset_metadata(data_service, TEST_TABLE_NAME, dataset_name)

    config = {**extra_config, "dataset_id": TEST_TABLE_NAME, "data_service": data_service}

    config["operation_variables"] = {**extra_operation_variables}
    config["operation_variables"]["$constant"] = SqlOperationResult(query="SELECT 'A'", type="constant", subtype="Char")
    config["operation_variables"]["$number"] = SqlOperationResult(query="SELECT 1.0", type="constant", subtype="Num")
    config["operation_variables"]["$date"] = SqlOperationResult(
        query="SELECT '2025-09-09'", type="constant", subtype="Char"
    )
    config["operation_variables"]["$list"] = SqlOperationResult(
        query="SELECT column1 FROM (VALUES ('A'), ('B'))", type="collection", subtype="Char"
    )

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
