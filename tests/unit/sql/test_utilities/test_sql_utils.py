import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)


def test_invalid_table_name():
    """Test that an exception is raised when an invalid table name is used."""
    data_service = PostgresQLDataService.instance(sql_namespace="")
    with pytest.raises(Exception) as e:
        PostgresQLDataService.add_test_dataset(
            data_service, table_name="SELECT", column_data={"key": [1]}, standards_context=DefaultStandardsContext()
        )
    assert isinstance(e.value, ValueError)


def test_uneven_columns():
    """Test that an exception is raised when the test data has columns with different lengths."""
    data_service = PostgresQLDataService.instance()
    with pytest.raises(Exception) as e:
        PostgresQLDataService.add_test_dataset(
            data_service,
            table_name="test",
            column_data={"key": [1], "value": [1, 2]},
            standards_context=DefaultStandardsContext(),
        )
    assert isinstance(e.value, ValueError)


def test_reserved_word_column_name():
    """A column named after a reserved SQL keyword is loaded under a generated identifier and keeps its name."""
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="test", column_data={"select": [1, 2]}, standards_context=DefaultStandardsContext()
    )
    schema = data_service.pgi.schema.get_table("test")
    column_hash = schema.get_column_hash("select")
    assert column_hash != "select"

    data_service.pgi.execute_sql(f"SELECT {column_hash} AS value FROM {schema.hash} ORDER BY id")
    assert [row["value"] for row in data_service.pgi.fetch_all()] == [1, 2]


def test_data_contains_id():
    """
    Test that an exception is raised when the test data has an 'id' column, as this is used as a row id by postgres.
    """
    data_service = PostgresQLDataService.instance()
    with pytest.raises(Exception) as e:
        PostgresQLDataService.add_test_dataset(
            data_service, table_name="test", column_data={"id": [1]}, standards_context=DefaultStandardsContext()
        )
    assert isinstance(e.value, ValueError)


def test_adding_invalid_column():
    """
    Test that an exception is raised when trying to add an column which is reserved in SQL.
    """
    data_service = PostgresQLDataService.instance()
    schema = PostgresQLDataService.add_test_dataset(
        data_service, table_name="test", column_data={"key": [1]}, standards_context=DefaultStandardsContext()
    )
    with pytest.raises(Exception) as e:
        data_service.pgi.add_column(
            schema.name,
            SqlColumnSchema(
                name="select",
                hash="select",
                type="Num",
            ),
        )
    assert isinstance(e.value, ValueError)


def test_adding_id_column():
    """
    Test that an exception is raised when trying to add an "id" column.
    """
    data_service = PostgresQLDataService.instance()
    schema = PostgresQLDataService.add_test_dataset(
        data_service, table_name="test", column_data={"key": [1]}, standards_context=DefaultStandardsContext()
    )
    with pytest.raises(Exception) as e:
        data_service.pgi.add_column(
            schema.name,
            SqlColumnSchema(
                name="id",
                hash="id",
                type="Num",
            ),
        )
    assert isinstance(e.value, ValueError)
