from typing import Union

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema


class SqlDbSchema:
    """Stores the schema for all of the SQL tables."""

    def __init__(self):
        self._tables: dict[str, SqlTableSchema] = {}

    def add_table(
        self,
        schema: SqlTableSchema,
    ) -> None:
        self._tables[schema.name.lower()] = schema

    def get_table(self, table: str) -> Union[SqlTableSchema, None]:
        return self._tables.get(table.lower())

    def get_table_hash(self, table: str) -> Union[str, None]:
        table_schema = self.get_table(table)
        if table_schema:
            return table_schema.hash
        return None

    def get_column(self, table: str, column: str) -> Union[SqlColumnSchema, None]:
        table_schema = self.get_table(table)
        if table_schema:
            return table_schema.get_column(column)
        return None

    def get_column_hash(self, table: str, column: str) -> Union[str, None]:
        table_schema = self.get_table(table)
        if not table_schema:
            return None
        col = table_schema.get_column(column)
        if col:
            return col.hash
        return None

    def table_exists(self, table: str) -> bool:
        return self.get_table(table) is not None

    def column_exists(self, table: str, column: str) -> bool:
        return self.get_column(table, column) is not None
