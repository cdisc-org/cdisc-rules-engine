import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema


def test_invalid_table_name():
    """Test that an exception is raised when an invalid table name is used."""
    data_service = PostgresQLDataService.instance()
    with pytest.raises(Exception) as e:
        PostgresQLDataService.add_test_dataset(data_service, table_name="SELECT", column_data={"key": [1]})
    assert isinstance(e.value, ValueError)


def test_uneven_columns():
    """Test that an exception is raised when the test data has columns with different lengths."""
    data_service = PostgresQLDataService.instance()
    with pytest.raises(Exception) as e:
        PostgresQLDataService.add_test_dataset(
            data_service, table_name="test", column_data={"key": [1], "value": [1, 2]}
        )
    assert isinstance(e.value, ValueError)


def test_invalid_column_name():
    """Test that an exception is raised when the test data has a column with an invalid name."""
    data_service = PostgresQLDataService.instance()
    with pytest.raises(Exception) as e:
        PostgresQLDataService.add_test_dataset(data_service, table_name="test", column_data={"select": [1]})
    assert isinstance(e.value, ValueError)


def test_data_contains_id():
    """
    Test that an exception is raised when the test data has an 'id' column, as this is used as a row id by postgres.
    """
    data_service = PostgresQLDataService.instance()
    with pytest.raises(Exception) as e:
        PostgresQLDataService.add_test_dataset(data_service, table_name="test", column_data={"id": [1]})
    assert isinstance(e.value, ValueError)


def test_adding_invalid_column():
    """
    Test that an exception is raised when trying to add an column which is reserved in SQL.
    """
    data_service = PostgresQLDataService.instance()
    schema = PostgresQLDataService.add_test_dataset(data_service, table_name="test", column_data={"key": [1]})
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
    schema = PostgresQLDataService.add_test_dataset(data_service, table_name="test", column_data={"key": [1]})
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
