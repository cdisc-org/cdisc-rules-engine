from typing import NamedTuple, Tuple, Union


class CompileUnsupported(Exception):
    """A check operator can't be expressed as part of one compiled query, so the rule runs the row-by-row path."""


class GroupJoin(NamedTuple):
    """A grouped operation's per-group values."""

    alias: str
    query: str
    on: str


class SqlTruth:
    """A per-row truth value in a SQL boolean expression."""

    __slots__ = ("sql", "joins")

    def __init__(self, sql: str, joins: Tuple[GroupJoin, ...] = ()):
        self.sql = sql
        self.joins = joins

    @classmethod
    def of(cls, expression: str, joins: Tuple[GroupJoin, ...] = ()) -> "SqlTruth":
        return cls(f"COALESCE(({expression}), FALSE)", tuple(joins))

    def from_clause(self, table_sql: str, alias: str) -> str:
        joins = "".join(f"\nLEFT JOIN ({join.query}) AS {join.alias} ON {join.on}" for join in self.joins)
        return f"{table_sql} AS {alias}{joins}"

    def _combined(self, sql: str, other) -> "SqlTruth":
        joins = self.joins + tuple(join for join in _joins_of(other) if join not in self.joins)
        return SqlTruth(sql, joins)

    def __and__(self, other) -> "SqlTruth":
        return self._combined(f"({self.sql} AND {_sql_of(other)})", other)

    def __rand__(self, other) -> "SqlTruth":
        return self._combined(f"({_sql_of(other)} AND {self.sql})", other)

    def __or__(self, other) -> "SqlTruth":
        return self._combined(f"({self.sql} OR {_sql_of(other)})", other)

    def __ror__(self, other) -> "SqlTruth":
        return self._combined(f"({_sql_of(other)} OR {self.sql})", other)

    def __invert__(self) -> "SqlTruth":
        return SqlTruth(f"(NOT {self.sql})", self.joins)

    def __bool__(self):
        raise TypeError("A SQL truth value has no Python truth value: it is only known once the query runs")

    def __repr__(self):
        return f"SqlTruth({self.sql})"


def _sql_of(value: Union[SqlTruth, bool]) -> str:
    if isinstance(value, SqlTruth):
        return value.sql
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    raise CompileUnsupported(f"Can't combine a SQL truth value with {type(value).__name__}")


def _joins_of(value) -> Tuple[GroupJoin, ...]:
    return value.joins if isinstance(value, SqlTruth) else ()
