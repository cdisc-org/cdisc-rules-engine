from typing import Any, Union

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema


class SqlTableSchema:
    """Stores the schema for a SQL table."""

    def __init__(self, name: str):
        self.name = name
        self._columns: dict[str, SqlColumnSchema] = {}

    def add_column(self, column: str, data: SqlColumnSchema) -> None:
        self._columns[column.lower()] = data

    def get_column(self, column: str) -> Union[SqlColumnSchema, None]:
        return self._columns.get(column.lower())

    def get_column_hash(self, column: str) -> Union[str, None]:
        col = self._columns.get(column.lower())
        if col:
            return col.hash
        return None

    @classmethod
    def from_data(cls, table_name: str, data: dict[str, Any]) -> "SqlTableSchema":
        """Create a SqlTableSchema from a dictionary."""
        instance = cls(table_name.lower())
        for column, value in data.items():
            instance.add_column(column.lower(), SqlColumnSchema.from_data(column, value))
        return instance

    @classmethod
    def from_metadata(cls, metadata: dict[str, Any]) -> "SqlTableSchema":
        """Create a SqlTableSchema from its metadata."""
        instance = cls(metadata.get("name").lower())
        for variable_metadata in metadata.get("variables", []):
            instance.add_column(variable_metadata.get("name").lower(), SqlColumnSchema.from_metadata(variable_metadata))
        return instance
