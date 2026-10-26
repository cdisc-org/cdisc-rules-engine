"""
Helpers for turning python rows into SQL a rule can select from.
Most rule types describe a dataset rather than read it (i.e., one row per variable, per define item, ...).
The rows are written out as a VALUES list inside a view.
That means nothing gets created or ALTER'd per rule and two rules wanting the same rows hit the same view.
"""

import hashlib
from typing import Dict, List, Optional, Sequence, Tuple

from pandas import isna as pd_isna  # arguably overkill for just checking NaN

from cdisc_rules_engine.data_service.sql_serialiser import SQLSerialiser
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

# bookkeeping metadata columns we're very familiar with by now
# never treated as if they were variables
NOT_DATA_COLUMNS = ("id", "source_ds", "source_row_number")


def unpivoted_columns(source_schema: SqlTableSchema) -> List[Tuple[str, SqlColumnSchema]]:
    """dataset's actual variables (data columns) in table order."""
    return [
        (name, column)
        for name, column in source_schema.get_columns()
        if name.lower() not in NOT_DATA_COLUMNS and column.origin == "data"
    ]


def literal(value) -> str:
    """render a python value the way inserting it into a table would have stored it."""
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    # pd.isna blows up on lists/dicts (it answers element-wise)
    # To avoid errors, only ask it about scalars
    if value is None or (not isinstance(value, (list, dict)) and pd_isna(value)):
        return "NULL"
    return str(value)


def typed(sql: str, column_type: str) -> str:
    return f"CAST({sql} AS {SQLSerialiser.column_type_to_sql_type(column_type)})"


def columns_of(column_types: Dict[str, str]) -> List[SqlColumnSchema]:
    return [SqlColumnSchema.generated(name, column_type) for name, column_type in column_types.items()]


def rows_body(columns: Sequence[SqlColumnSchema], rows: List[dict]) -> str:
    """
    SELECT over literal rows, behaving like a table of `columns` with `rows` inserted:
      - ids are 1..n in list order
      - every cell is cast to its column's type
      - only keys present on the *first* row are written, the rest come out NULL

    That last one looks odd i know, but it's what the old builders got from SQLSerialiser, which takes
    its insert column list from data[0].
    I'm choosing to keep it so the views match what rules were tested on.
    """
    names = ", ".join(["id", *[column.hash for column in columns]])
    if not rows:
        # still need typed columns, otherwise pg reads every column of an empty view as text
        empty = ", ".join(["CAST(NULL AS BIGINT) AS id", *[typed("NULL", c.type) + f" AS {c.hash}" for c in columns]])
        return f"SELECT {empty} WHERE FALSE"

    written = {key.lower() for key in rows[0]}
    values = []
    for index, row in enumerate(rows, start=1):
        cells = []
        for column in columns:
            key = column.name.lower()
            cells.append(typed(literal(row.get(key)) if key in written else "NULL", column.type))
        values.append(f"({', '.join([str(index), *cells])})")
    # TODO: a study with a huge define could make this VALUES list very long.
    # Hasn't been a problem yet, but if it is, COPY-ing into a temp table would be the obvious fallback imo.
    return f"SELECT * FROM (VALUES {', '.join(values)}) AS rows({names})"


def empty_body(columns: Sequence[SqlColumnSchema]) -> str:
    return rows_body(columns, [])


def ensure_view(
    pgi,
    body: str,
    columns: Sequence[SqlColumnSchema],
    source: str = "derived",
    name: Optional[str] = None,
    materialise: bool = False,
) -> str:
    """
    Register body as a view and return its table id.

    Unnamed views are keyed on a hash of their SQL so the same rows asked for twice are only
    created once.
    A named view is the study's single view of that name.
    Operations can point at it like a domain (see the global value check).

    materialise=True writes a real table instead.
    Only worth it for rows that are expensive to produce and shared by every rule in the study.
    A single dataset's unpivot stays a view: it's read once, and writing millions of values out costs a
    lot more than scanning them.
    """
    name = name or "rule_source_" + hashlib.md5(body.encode("utf-8")).hexdigest()[:20]
    if pgi.schema.get_table(name) is not None:
        return name

    schema = SqlTableSchema.derived(name, pgi)
    schema.source = source
    for column in columns:
        schema.add_column(column)
    if materialise:
        # cannot afford writing to the WAL
        pgi.execute_sql(f"CREATE UNLOGGED TABLE {schema.hash} AS {body}")
        pgi.execute_sql(f"ANALYZE {schema.hash}")
    else:
        pgi.execute_sql(f"CREATE OR REPLACE VIEW {schema.hash} AS {body}")
    pgi.schema.add_table(schema)
    return name


def rows_view(context, column_types: Dict[str, str], rows: List[dict], name: Optional[str] = None) -> str:
    columns = columns_of(column_types)
    return ensure_view(context.pgi, rows_body(columns, rows), columns, name=name)
