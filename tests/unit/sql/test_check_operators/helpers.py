"""Helper functions for SQL operator tests."""

import pandas as pd
from psycopg2 import Error as PostgresError

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.compiler.conditions import CO, ConditionCompiler
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.exceptions.custom_exceptions import SqlOperatorError
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)

TEST_TABLE_NAME = "test_table"

#: The keys of an operator's config the compiler supplies itself
COMPILER_KEYS = {"dataset_id", "data_service", "dataset_metadata", "operation_variables"}


class CompiledOperators:
    """
    Runs one check operator as a compiled rule would: the condition is compiled to SQL and
    evaluated for every row of the table, giving the truth value of each row.
    """

    def __init__(self, config: dict):
        self.data = config

    def __getattr__(self, name):
        if name not in PostgresQLOperators._operator_map:
            return getattr(PostgresQLOperators(self.data), name)
        return lambda other_value: run_operator(self.data, name, other_value)


def run_operator(config: dict, operator: str, other_value: dict) -> pd.Series:
    """The truth value of one check for each row of config's dataset, ordered by row."""
    data_service = config["data_service"]
    dataset_id = config["dataset_id"]
    dataset_metadata = config.get("dataset_metadata") or data_service.get_dataset_metadata(dataset_id)
    extra = {key: value for key, value in config.items() if key not in COMPILER_KEYS}
    compiler = ConditionCompiler(
        data_service, dataset_id, dataset_metadata, config.get("operation_variables") or {}, extra_operator_data=extra
    )
    compiled = compiler.compile_condition(operator, other_value)

    pgi = data_service.pgi
    source = compiled.from_clause(pgi.schema.get_table_hash(dataset_id))
    try:
        pgi.execute_sql(f"SELECT {CO}.id AS id, ({compiled.where}) AS data FROM {source} ORDER BY {CO}.id")
        rows = pgi.fetch_all()
        pgi.execute_sql(f"SELECT {CO}.id AS id FROM {source} WHERE {compiled.where} ORDER BY {CO}.id")
        selected = [row["id"] for row in pgi.fetch_all()]
    except PostgresError as error:
        raise SqlOperatorError(original_exception=error, operator_name=operator) from error

    assert selected == [row["id"] for row in rows if row["data"] is True], "WHERE and SELECT disagree"
    return pd.Series(data={row["id"] - 1: row["data"] for row in rows})


def create_sql_operators(
    column_data: dict, extra_operation_variables: dict = {}, extra_config: dict = {}, dataset_name: str = None
) -> CompiledOperators:
    """Create a test dataset and an operators instance that evaluates checks against it.
    It will preload some operation variables which can be used in tests.

    Args:
        column_data: Dictionary containing column names and their data
        extra_operation_variables: Optional additional custom dictionary of operation variables
        extra_config: Optional additional configuration for the operators
        dataset_name: Optional dataset name for testing dataset_name column operations

    Returns:
        An object whose attributes are the check operators, each returning one truth value per row
    """
    standards_context = DefaultStandardsContext()
    data_service = PostgresQLDataService.instance()

    table_name = dataset_name or TEST_TABLE_NAME
    PostgresQLDataService.add_test_dataset(
        data_service, table_name=table_name, column_data=column_data, standards_context=standards_context
    )

    config = {**extra_config, "dataset_id": table_name, "data_service": data_service}

    config["operation_variables"] = {**extra_operation_variables}
    config["operation_variables"]["$constant"] = SqlOperationResult(query="SELECT 'A'", type="constant", subtype="Char")
    config["operation_variables"]["$number"] = SqlOperationResult(query="SELECT 1.0", type="constant", subtype="Num")
    config["operation_variables"]["$date"] = SqlOperationResult(
        query="SELECT '2025-09-09'", type="constant", subtype="Char"
    )
    config["operation_variables"]["$list"] = SqlOperationResult(
        query="SELECT value FROM (VALUES ('A'), ('B')) as t(value)", type="collection", subtype="Char"
    )
    config["operation_variables"]["$empty_date"] = SqlOperationResult(
        query="SELECT NULL", type="constant", subtype="Date"
    )

    config["dataset_metadata"] = data_service.get_dataset_metadata(
        table_name,
    )

    return CompiledOperators(config)


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
