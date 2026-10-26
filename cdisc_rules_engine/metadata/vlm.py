"""
Define.xml value level metadata applies to a value when its record matches the VLM's where
clause.
Every range check of the clause, AND'd, as SQL over the dataset's records.
"""

from typing import List

from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

NUMERIC_COMPARATORS = {"LT": "<", "LE": "<=", "GT": ">", "GE": ">=", "EQ": "=", "NE": "<>"}


class UntranslatableWhereClause(Exception):
    """Raised when a Define where clause cannot be expressed as SQL against the source dataset."""


def where_clause_to_sql(where_clause: List[dict], source_schema: SqlTableSchema, source_alias: str) -> str:
    """ANDs every range check of the where clause."""
    if not where_clause:
        raise UntranslatableWhereClause("where clause is missing or references an item not in the define")
    conditions = []
    for check in where_clause:
        column = source_schema.get_column(check["variable"])
        if column is None:
            raise UntranslatableWhereClause(f"where clause variable '{check['variable']}' is not in the dataset")
        condition = range_check_to_sql(
            f"{source_alias}.{column.hash}", column.type, check["comparator"], check["values"]
        )
        conditions.append(f"({condition})")
    return " AND ".join(conditions)


def range_check_to_sql(column: str, column_type: str, comparator: str, values: List[str]) -> str:
    """
    Missing values behave the same for numeric and text columns.
    They never satisfy an ordering comparator, they equal an empty check value, and they satisfy NE / NOTIN.
    """
    if not values:
        raise UntranslatableWhereClause(f"range check with comparator '{comparator}' has no check values")
    if comparator in ("LT", "LE", "GT", "GE"):
        return ordering_to_sql(column, column_type, comparator, values[0])
    if comparator in ("EQ", "NE"):
        values = values[:1]
    elif comparator not in ("IN", "NOTIN"):
        raise UntranslatableWhereClause(f"unsupported comparator '{comparator}'")

    matches = f"COALESCE({membership_to_sql(column, column_type, values)}, FALSE)"
    return matches if comparator in ("EQ", "IN") else f"NOT {matches}"


def ordering_to_sql(column: str, column_type: str, comparator: str, value: str) -> str:
    if column_type == "Num":
        try:
            return f"{column} {NUMERIC_COMPARATORS[comparator]} {float(value)!r}"
        except ValueError:
            raise UntranslatableWhereClause(f"'{value}' is not numeric but is compared to a numeric column")
    literal = "'" + str(value).replace("'", "''") + "'"
    return f"COALESCE(NULLIF(CAST({column} AS TEXT), '') {NUMERIC_COMPARATORS[comparator]} {literal}, FALSE)"


def membership_to_sql(column: str, column_type: str, values: List[str]) -> str:
    """
    True when the column equals one of the values.
    NULL is only matched by ''.
    """
    if column_type != "Num":
        literals = ["'" + str(value).replace("'", "''") + "'" for value in values]
        return f"COALESCE(CAST({column} AS TEXT), '') IN ({', '.join(literals)})"

    numbers = []
    matches_missing = False
    for value in values:
        if str(value).strip() == "":
            matches_missing = True
            continue
        try:
            numbers.append(repr(float(value)))
        except ValueError:
            pass
    parts = []
    if numbers:
        parts.append(f"{column} IN ({', '.join(numbers)})")
    if matches_missing:
        parts.append(f"{column} IS NULL")
    return " OR ".join(parts) if parts else "FALSE"
