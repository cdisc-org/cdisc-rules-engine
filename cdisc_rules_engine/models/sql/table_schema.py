from collections import OrderedDict
from typing import Any, Literal, Tuple, Union, TYPE_CHECKING

from cdisc_rules_engine.data_service.util import generate_hash
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema

if TYPE_CHECKING:
    from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface


class SqlTableSchema:
    """Stores the schema for a SQL table."""

    def __init__(self, name: str, hash: str, source: Literal["data", "derived", "static"]):
        self.name = name
        self.hash = hash
        self._columns: OrderedDict[str, SqlColumnSchema] = OrderedDict()
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
        """Return columns in insertion order."""
        return list(self._columns.items())

    @classmethod
    def from_data(cls, table_name: str, data: dict[str, Any], pgi: "PostgresQLInterface") -> "SqlTableSchema":
        """Create a SqlTableSchema from a dictionary."""
        # Check for reserved column names in user data
        for column in data.keys():
            if column.lower() == "id":
                raise ValueError("Column name 'id' is reserved for primary key in SQL tables.")

        prefix = pgi.sql_namespace
        hash_name = f"{prefix}_{table_name.lower()}" if prefix else table_name.lower()
        instance = cls(table_name.lower(), hash_name, source="data")
        for column, value in data.items():
            instance.add_column(SqlColumnSchema.from_data(column, value))
        return instance

    @classmethod
    def from_metadata(cls, metadata: DatasetMetadata2, pgi: "PostgresQLInterface") -> "SqlTableSchema":
        """Create a SqlTableSchema from its metadata."""
        # Check for reserved column names in user data
        for column in metadata.variables:
            if column.name.lower() == "id":
                raise ValueError("Column name 'id' is reserved for primary key in SQL tables.")

        prefix = pgi.sql_namespace
        hash_name = f"{prefix}_{metadata.name.lower()}" if prefix else metadata.name.lower()
        instance = cls(metadata.name.lower(), hash_name, source="data")
        for variable_metadata in metadata.variables:
            instance.add_column(SqlColumnSchema.from_metadata(variable_metadata))
        return instance

    @classmethod
    def derived(cls, name: str, pgi: "PostgresQLInterface") -> "SqlTableSchema":
        """Create a SqlTableSchema for a join operation or non-record dataset builder."""
        hash = generate_hash(name.lower())
        prefix = pgi.sql_namespace
        hash_name = f"{prefix}_{hash}" if prefix else hash
        return cls(name.lower(), hash_name, source="derived")

    @classmethod
    def static(cls, name: str) -> "SqlTableSchema":
        """Create a SqlTableSchema for a static table (ie an implementation guide table)."""
        return cls(name.lower(), name.lower(), source="static")
