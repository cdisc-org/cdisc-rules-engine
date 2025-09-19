from typing import Any, Literal, Tuple, Union

from cdisc_rules_engine.data_service.util import generate_hash
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema


class SqlTableSchema:
    """Stores the schema for a SQL table."""

    def __init__(self, name: str, hash: str, source: Literal["data", "derived", "static"]):
        self.name = name
        self.hash = hash
        self._columns: dict[str, SqlColumnSchema] = {}
        self.source = source

        id_column = SqlColumnSchema(name="id", hash="id", type="Num")
        self.add_column(id_column)

    def add_column(self, data: SqlColumnSchema) -> None:
        self._columns[data.name.lower()] = data

    def get_column(self, column: str) -> Union[SqlColumnSchema, None]:
        if column is None:
            return None
        return self._columns.get(column.lower())

    def has_column(self, column: str) -> bool:
        return self.get_column(column) is not None

    def get_column_hash(self, column: str) -> Union[str, None]:
        col = self.get_column(column)
        if col:
            return col.hash
        return None

    def get_columns(self) -> list[Tuple[str, SqlColumnSchema]]:
        return list(self._columns.items())

    @classmethod
    def from_data(cls, table_name: str, data: dict[str, Any]) -> "SqlTableSchema":
        """Create a SqlTableSchema from a dictionary."""
        # Check for reserved column names in user data
        for column in data.keys():
            if column.lower() == "id":
                raise ValueError("Column name 'id' is reserved for primary key in SQL tables.")

        instance = cls(table_name.lower(), table_name.lower(), source="data")
        for column, value in data.items():
            instance.add_column(SqlColumnSchema.from_data(column, value))
        return instance

    @classmethod
    def from_metadata(cls, metadata: dict[str, Any]) -> "SqlTableSchema":
        """Create a SqlTableSchema from its metadata."""
        instance = cls(metadata.get("name").lower(), metadata.get("name").lower(), source="data")
        for variable_metadata in metadata.get("variables", []):
            instance.add_column(SqlColumnSchema.from_metadata(variable_metadata))
        return instance

    @classmethod
    def from_join(cls, name: str) -> "SqlTableSchema":
        """Create a SqlTableSchema for a join operation."""
        hash = generate_hash(name.lower())
        return cls(name.lower(), hash, source="derived")

    @classmethod
    def static(cls, name: str) -> "SqlTableSchema":
        """Create a SqlTableSchema for a static table (ie an implementation guide table)."""
        return cls(name.lower(), name.lower(), source="static")
