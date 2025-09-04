from typing import Any, Tuple, Union

from cdisc_rules_engine.data_service.util import generate_hash
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema


class SqlTableSchema:
    """Stores the schema for a SQL table."""

    def __init__(self, name: str, hash: str):
        self.name = name
        self.hash = hash
        self._columns: dict[str, SqlColumnSchema] = {}

    def add_column(self, data: SqlColumnSchema) -> None:
        self._columns[data.name.lower()] = data

    def get_column(self, column: str) -> Union[SqlColumnSchema, None]:
        return self._columns.get(column.lower())

    def get_column_hash(self, column: str) -> Union[str, None]:
        col = self._columns.get(column.lower())
        if col:
            return col.hash
        return None

    def get_columns(self) -> list[Tuple[str, SqlColumnSchema]]:
        return list(self._columns.items())

    @classmethod
    def from_data(cls, table_name: str, data: dict[str, Any]) -> "SqlTableSchema":
        """Create a SqlTableSchema from a dictionary."""
        instance = cls(table_name.lower(), table_name.lower())
        for column, value in data.items():
            instance.add_column(SqlColumnSchema.from_data(column, value))
        return instance

    @classmethod
    def from_metadata(cls, metadata: dict[str, Any]) -> "SqlTableSchema":
        """Create a SqlTableSchema from its metadata."""
        instance = cls(metadata.get("name").lower(), metadata.get("name").lower())
        for variable_metadata in metadata.get("variables", []):
            instance.add_column(SqlColumnSchema.from_metadata(variable_metadata))
        return instance

    @classmethod
    def from_join(cls, name: str) -> "SqlTableSchema":
        """Create a SqlTableSchema for a join operation."""
        hash = generate_hash(name.lower())
        return cls(name.lower(), hash)
