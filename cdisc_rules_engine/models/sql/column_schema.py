from typing import Any

from cdisc_rules_engine.data_service.util import generate_hash
from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES


class SqlColumnSchema:
    """Stores the schema for a single SQL column."""

    def __init__(self, name: str, hash: str, type: DATASET_COLUMN_TYPES):
        self.name = name
        self.hash = hash
        self.type = type

    @staticmethod
    def python_to_sql_type(value: Any) -> str:
        """Map python types to SQL types."""
        if isinstance(value, (int, float)):
            return "Num"
        elif isinstance(value, str):
            return "Char"
        else:
            raise ValueError(f"Unsupported type: {type(value)}")

    @staticmethod
    def sas_to_sql_type(type: str) -> str:
        """Map sas types to SQL types."""
        if type.lower() in ("char", "s"):
            return "Char"
        elif type.lower() in ("num", "numeric", "d"):
            return "Num"
        else:
            raise ValueError(f"Unsupported type: {type}")

    @classmethod
    def from_data(cls, column: str, data: Any) -> "SqlColumnSchema":
        sql_type = cls.python_to_sql_type(data)
        return cls(name=column.lower(), hash=column.lower(), type=sql_type)

    @classmethod
    def from_metadata(cls, metadata: dict[str, Any]) -> "SqlColumnSchema":
        name = metadata.get("name").lower()
        sql_type = cls.sas_to_sql_type(metadata.get("type"))
        return cls(name=name, hash=name, type=sql_type)

    @classmethod
    def generated(cls, column: str, type: DATASET_COLUMN_TYPES) -> "SqlColumnSchema":
        """Create a SqlColumnSchema with a generated hash."""
        hash = generate_hash(column.lower())
        return cls(name=column.lower(), hash=hash, type=type)
