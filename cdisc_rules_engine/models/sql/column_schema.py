from typing import Any

from cdisc_rules_engine.data_service.util import generate_hash
from cdisc_rules_engine.models.dataset_metadata2 import VariableMetadata
from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES, ORIGIN_COLUMN_TYPES


class SqlColumnSchema:
    """Stores the schema for a single SQL column."""

    def __init__(
        self,
        name: str,
        hash: str,
        type: DATASET_COLUMN_TYPES,
        alias: bool = False,
        origin: ORIGIN_COLUMN_TYPES = "data",
    ):
        self.name = name
        self.hash = hash
        self.type = type
        self.origin = origin
        self.alias = alias

    @staticmethod
    def python_to_sql_type(value: Any) -> str:
        """Map python types to SQL types."""
        if isinstance(value, (int, float)):
            return "Num"
        elif isinstance(value, str):
            return "Char"
        else:
            raise ValueError(f"Unsupported type: {type(value)}")

    @classmethod
    def from_data(cls, column: str, data: Any) -> "SqlColumnSchema":
        sql_type = cls.python_to_sql_type(data)
        return cls(name=column.lower(), hash=column.lower(), type=sql_type)

    @classmethod
    def from_metadata(cls, metadata: VariableMetadata) -> "SqlColumnSchema":
        name = metadata.name.lower()
        return cls(name=name, hash=name, type=metadata.type)

    @classmethod
    def generated(cls, column: str, type: DATASET_COLUMN_TYPES) -> "SqlColumnSchema":
        """Create a SqlColumnSchema with a generated hash."""
        hash = generate_hash(column.lower())
        return cls(name=column.lower(), hash=hash, type=type)

    @classmethod
    def check_operator(cls, column: str) -> "SqlColumnSchema":
        """Create a SqlColumnSchema with the check operator origin."""
        hash = generate_hash(column.lower())
        return cls(name=column.lower(), hash=hash, type="Bool", origin="co")

    @classmethod
    def define(cls, column: str, type: DATASET_COLUMN_TYPES) -> "SqlColumnSchema":
        """Create a SqlColumnSchema with the define origin."""
        hash = generate_hash(column.lower())
        return cls(name=column.lower(), hash=hash, type=type, origin="define")

    @classmethod
    def alias(cls, column: str, schema: "SqlColumnSchema") -> "SqlColumnSchema":
        """Create a SqlColumnSchema which aliases another column."""
        return cls(name=column.lower(), hash=schema.hash, type=schema.type, alias=True)
