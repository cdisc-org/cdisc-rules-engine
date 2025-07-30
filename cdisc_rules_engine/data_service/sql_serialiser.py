from typing import List, Tuple, Dict, Any, Optional


class SQLSerialiser:
    """Convert Python objects to SQL statements"""

    @staticmethod
    def python_to_sql_type(value: Any) -> str:
        """Map Python types to PostgreSQL types"""
        if isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        elif isinstance(value, bool):
            return "BOOLEAN"
        else:
            return "TEXT"

    @classmethod
    def create_table_from_dict(
        cls, table_name: str, sample_dict: Dict[str, Any], primary_key: Optional[str] = None
    ) -> str:
        """Generate CREATE TABLE statement from a dictionary"""
        columns = []

        for key, value in sample_dict.items():
            col_type = cls.python_to_sql_type(value)
            col_def = f"{key} {col_type}"

            if key == primary_key:
                col_def += " PRIMARY KEY"

            columns.append(col_def)

        columns_sql = ",\n    ".join(columns)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n id SERIAL PRIMARY KEY, {columns_sql}\n);"

    @classmethod
    def insert_dict(cls, table_name: str, data: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """Generate INSERT statement from a dictionary"""
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
