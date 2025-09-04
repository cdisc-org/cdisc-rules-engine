from typing import Any, Dict, List, Tuple

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
            case _:
                raise ValueError(f"Unsupported column type: {type(value)}")

    @classmethod
    def create_table_query_from_schema(cls, schema: "SqlTableSchema") -> str:  # , primary_key: Optional[str] = None
        """Generate CREATE TABLE statement from a schema"""

        if schema.hash.upper() in SQL_RESERVED_KEYWORDS:
            raise ValueError(f"Table name '{schema.hash}' is a reserved SQL keyword.")

        column_definitions = []

        for column in schema._columns.values():
            if column.hash.upper() in SQL_RESERVED_KEYWORDS:
                raise ValueError(f"Column name '{schema.hash}' is a reserved SQL keyword.")
            if column.hash.lower() == "id":
                raise ValueError("Column name 'id' is reserved for primary key in SQL tables.")

            sql_type = cls.column_type_to_sql_type(column.type)
            col_def = f"{column.hash} {sql_type}"
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
    def insert_dict(cls, table_name: str, data: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """Generate INSERT statement from a dictionary"""

        # lowercase the columns:
        columns = list(data.keys())
        values = [data[col] for col in columns]

        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)

        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        return query, values

    @classmethod
    def insert_many_dicts(cls, table_name: str, data: List[Dict[str, Any]]) -> Tuple[str, List[List[Any]]]:
        """Generate INSERT statement for multiple dictionaries"""
        if not data:
            raise ValueError("Data list cannot be empty")

        columns = list(data[0].keys())
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        values = []
        for row in data:
            row_values = [row.get(col) for col in columns]
            values.append(row_values)

        return query, values

    @classmethod
    def create_column_from_schema(cls, table_schema: SqlTableSchema, column_schema: "SqlColumnSchema") -> str:
        """Generate ALTER TABLE statement from a schema"""

        if column_schema.hash.upper() in SQL_RESERVED_KEYWORDS:
            raise ValueError(f"Column name '{column_schema.hash}' is a reserved SQL keyword.")
        if column_schema.hash.lower() == "id":
            raise ValueError("Column name 'id' is reserved for primary key in SQL tables.")

        sql_type = cls.column_type_to_sql_type(column_schema.type)
        return f"ALTER TABLE {table_schema.hash} ADD COLUMN {column_schema.hash} {sql_type};"
