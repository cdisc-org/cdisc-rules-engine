"""
Bits a compiled rule adds around its source rows: window columns computed in the source
subquery, and relations joined onto the rows. See conditions.py for why these exist.
"""

import itertools
from dataclasses import dataclass
from typing import Dict, List, NamedTuple

from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

SOURCE_ALIAS = "t"
DERIVED_PREFIX = "w_"


class GroupJoin(NamedTuple):
    """e.g. a grouped operation's value per group, LEFT JOIN'd onto the rows being checked."""

    alias: str
    query: str
    on: str


@dataclass(frozen=True)
class DerivedColumn:
    name: str
    expression: str
    type: DATASET_COLUMN_TYPES

    def render(self, column_hash: str) -> str:
        return f"{self.expression} AS {column_hash}"


class DerivedColumnRegistry:
    """
    Filled in by operators while the WHERE clause is compiled.
    Two operators asking for the same expression (same name + sql) share one column.
    """

    def __init__(self, schema: SqlTableSchema, alias: str):
        self.schema = schema
        self.alias = alias
        self._columns: Dict[str, DerivedColumn] = {}

    def add(self, name: str, expression: str, column_type: DATASET_COLUMN_TYPES = "Bool") -> str:
        """Returns the SQL to read the column back e.g. co.<hash>."""
        name = self._unique_name(name, expression)
        if name not in self._columns:
            self._columns[name] = DerivedColumn(name, expression, column_type)
            self.schema.add_column(SqlColumnSchema.generated(name, column_type))
        return f"{self.alias}.{self.schema.get_column_hash(name)}"

    def _unique_name(self, name: str, expression: str) -> str:
        # w_ prefix so we don't collide with real variables
        # numeric suffix if the name's already taken by a real column or
        # by a different expression
        base = f"{DERIVED_PREFIX}{name.lower()}"
        for suffix in itertools.count():
            candidate = f"{base}_{suffix}" if suffix else base
            existing = self._columns.get(candidate)
            if existing is not None:
                if existing.expression == expression:
                    return candidate
            elif not self.schema.has_column(candidate):
                return candidate

    def select_items(self) -> List[str]:
        return [column.render(self.schema.get_column_hash(name)) for name, column in self._columns.items()]

    def __bool__(self) -> bool:
        return bool(self._columns)

    def __len__(self) -> int:
        return len(self._columns)
