from typing import Any, Callable, Dict, List, Tuple

import pandas as pd

from cdisc_rules_engine.data_service.reserved_keywords import SQL_RESERVED_KEYWORDS
from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import (
    SqlTableSchema,
)


class SQLSerialiser:
    """Convert Python objects to SQL statements"""

    @staticmethod
    def column_type_to_sql_type(value: DATASET_COLUMN_TYPES) -> str:
        """Map column types to SQL types."""
        match value:
            case "Char":
                return "TEXT"
            case "Num":
                return "DOUBLE PRECISION"
            case "Bool":
                return "BOOLEAN"
            case "Date":
                return "TIMESTAMP"
            case _:
                raise ValueError(f"Unsupported column type: {type(value)}")

    @classmethod
    def create_table_query_from_schema(cls, schema: "SqlTableSchema") -> str:  # , primary_key: Optional[str] = None
        """Generate CREATE TABLE statement from a schema"""

        if schema.hash.upper() in SQL_RESERVED_KEYWORDS:
            raise ValueError(f"Table name '{schema.hash}' is a reserved SQL keyword.")

        column_definitions = []

        for _, col_schema in schema.get_columns():
            # Skip alias columns
            if col_schema.alias:
                continue

            if col_schema.hash.lower() == "id":
                continue

            if col_schema.hash.upper() in SQL_RESERVED_KEYWORDS:
                raise ValueError(f"Column name '{col_schema.hash}' is a reserved SQL keyword.")

            sql_type = cls.column_type_to_sql_type(col_schema.type)
            col_def = f"{col_schema.hash} {sql_type}"
            column_definitions.append(col_def)

        if len(column_definitions) > 0:
            columns_sql = ",\n    ".join(column_definitions)
            return f"""CREATE TABLE IF NOT EXISTS {schema.hash} (
                    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY, {columns_sql}
                );"""
        else:
            return f"""CREATE TABLE IF NOT EXISTS {schema.hash} (
                        id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY
                    );"""

    @classmethod
    def insert_dict(cls, schema: SqlTableSchema, data: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """Generate INSERT statement from a dictionary"""
        return cls.insert_many_dicts(schema, [data])

    @classmethod
    def insert_many_dicts(cls, schema: SqlTableSchema, data: List[Dict[str, Any]]) -> Tuple[str, List[List[Any]]]:
        """Generate INSERT statement for multiple dictionaries"""
        if not data:
            raise ValueError("Data list cannot be empty")

        columns, map_fn = cls._generate_insertion_columns(schema, data[0])
        rows = [f"({map_fn(row)})" for row in data]

        return f"INSERT INTO {schema.hash} ({columns}) VALUES {", ".join(rows)}"

    @staticmethod
    def _generate_insertion_columns(
        schema: SqlTableSchema, example_row: Dict[str, Any]
    ) -> Tuple[str, Callable[[Dict[str, Any]], str]]:
        """
        Generates the list of columns and a function to convert a row to a SQL string.
        """
        example_row = {k.lower(): v for k, v in example_row.items()}
        column_names = []
        column_hashes = []
        for col_name, col_schema in schema.get_columns():
            if col_schema.alias or col_name.lower() == "id":
                continue
            if col_name in example_row:
                column_names.append(col_name)
                column_hashes.append(col_schema.hash)

        def map_row_to_sql(row: Dict[str, Any]) -> str:
            """
            Maps a row to a SQL string for insertion.
            """
            values = []
            for col_name in column_names:
                value = row.get(col_name.lower())
                if isinstance(value, str):
                    escaped_value = value.replace("'", "''")
                    value = f"'{escaped_value}'"
                elif value is None or pd.isna(value):
                    value = "NULL"
                values.append(str(value))
            return ", ".join(values)

        return ", ".join(column_hashes), map_row_to_sql

    @classmethod
    def create_column_from_schema(cls, table_schema: SqlTableSchema, column_schema: "SqlColumnSchema") -> str:
        """Generate ALTER TABLE statement from a schema"""

        if column_schema.hash.lower() == "id":
            raise ValueError("Column name 'id' is reserved for primary key in SQL tables and cannot be added.")

        if column_schema.hash.upper() in SQL_RESERVED_KEYWORDS:
            raise ValueError(f"Column name '{column_schema.hash}' is a reserved SQL keyword.")

        sql_type = cls.column_type_to_sql_type(column_schema.type)
        return f"ALTER TABLE {table_schema.hash} ADD COLUMN {column_schema.hash} {sql_type};"
