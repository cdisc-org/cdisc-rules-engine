"""
Value checks: one row per value of the dataset, i.e. the dataset unpivoted to
(row_number, variable_name, variable_value, ...metadata about that variable).
All of this stays a view over the dataset table.
See unpivot_body for how.
"""

from typing import Callable, Dict, List, Optional, Sequence

from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.lengths import variable_length_summary, wants_variable_length_summary
from cdisc_rules_engine.compiler.sources.views import (
    columns_of,
    empty_body,
    ensure_view,
    literal,
    typed,
    unpivoted_columns,
)
from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.metadata.store import DEFINE_VARIABLES_TYPE, LIBRARY_VARIABLES_TYPE
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema

VALUES_WITH_VARIABLE_METADATA = {
    "row_number": "Num",
    "variable_name": "Char",
    "variable_value": "Char",
    "variable_label": "Char",
    "variable_data_type": "Char",
    "variable_length": "Num",
    "variable_order_number": "Num",
    "variable_format": "Char",
    "variable_value_length": "Num",
}

PerVariable = Callable[[str, SqlColumnSchema], List[str]]


def unpivot_body(
    source_schema: SqlTableSchema,
    columns: Sequence[SqlColumnSchema],
    per_variable: PerVariable,
    row_number_type: str,
    leading: Sequence[str] = (),
    id_offset: int = 0,
) -> Optional[str]:
    """
    SELECT producing one row per (record, variable) of the dataset.

    How it works:

    CROSS JOIN LATERAL a VALUES list with one entry per variable.
    That way, each record fans out into one row per variable.
    The VALUES entry carries:
     - variable name
     - variable value for this record
     - whatever `per_variable` returns (metadata constants, usually)

    Layout of columns:
        [*leading, row_number, variable_name, variable_value, *per_variable columns]

    Ids are (k - 1) * n_records + record_position (+ id_offset): every value of the first
    variable, then every value of the second, and so on.
    Reported rows get sorted by id which preserves the order of the original dataset's records as well as any errors.

    Returns None when there's nothing to unpivot.
    """
    variables = unpivoted_columns(source_schema)
    if not variables:
        return None

    entries = []
    for ordinal, (name, column) in enumerate(variables, start=1):
        cells = [str(ordinal), literal(name.upper()), f"CAST(t.{column.hash} AS TEXT)", *per_variable(name, column)]
        entries.append(f"({', '.join(cells)})")

    n_leading = len(leading)
    extra = [f"v{index}" for index in range(len(columns) - 3 - n_leading)]
    # relative to the smallest id.
    # doesn't matter where the table's ids happen to start.
    position = "(t.id - records.first_id + 1)"
    select = [  # monstrous SELECT list for the unpivoted view
        f"{id_offset} + (j.ordinal - 1) * records.n + {position} AS id",
        *[f"{value} AS {column.hash}" for value, column in zip(leading, columns)],
        f"{typed(position, row_number_type)} AS {columns[n_leading].hash}",
        f"CAST(j.variable_name AS TEXT) AS {columns[n_leading + 1].hash}",
        f"j.variable_value AS {columns[n_leading + 2].hash}",
        *[f"{name} AS {column.hash}" for name, column in zip(extra, columns[n_leading + 3 :])],
    ]
    return (
        f"SELECT {', '.join(select)} FROM {source_schema.hash} t "
        f"CROSS JOIN (SELECT COUNT(*) AS n, MIN(id) AS first_id FROM {source_schema.hash}) records "
        f"CROSS JOIN LATERAL (VALUES {', '.join(entries)}) "
        f"AS j(ordinal, variable_name, variable_value{''.join(', ' + name for name in extra)})"
    )


def _unpivot_view(context: SourceContext, source_schema: SqlTableSchema, columns, per_variable, row_number_type):
    body = unpivot_body(source_schema, columns, per_variable, row_number_type)
    return ensure_view(context.pgi, body or empty_body(columns), columns)


def _source_schema(context: SourceContext) -> SqlTableSchema:
    table_id = context.rule_dataset()
    source_schema = context.pgi.schema.get_table(table_id)
    if not source_schema:
        raise ValueError(f"Source table {table_id} not found")
    return source_schema


# --- with variable / dataset metadata -------------------------------------------------------


def value_length_sql(column_hash: str, data_type: str) -> str:
    # leading zeros don't count towards a number's length, and for floats neither does the "."
    if data_type == "integer":
        return f"LENGTH(LTRIM(CAST(t.{column_hash} AS TEXT), '0'))"
    if data_type == "float":
        return f"LENGTH(REPLACE(LTRIM(CAST(t.{column_hash} AS TEXT), '0'), '.', ''))"
    return f"LENGTH(CAST(t.{column_hash} AS TEXT))"


def variable_value_metadata(variables) -> PerVariable:
    """per_variable callback: the variable's own metadata, plus the length of this value."""
    metadata = {
        variable.name.upper(): {
            "label": variable.label or "",
            "data_type": variable.type or "",
            "length": variable.length or 0,
            "order": variable.order or 0,
            "format": variable.format or "",
        }
        for variable in variables
    }

    def per_variable(name: str, column: SqlColumnSchema) -> List[str]:
        meta = metadata.get(name.upper(), {})
        data_type = meta.get("data_type", "")
        return [
            typed(literal(meta.get("label", "")), "Char"),
            typed(literal(data_type), "Char"),
            typed(literal(meta.get("length", 0)), "Num"),
            typed(literal(meta.get("order", 0)), "Num"),
            typed(literal(meta.get("format", "")), "Char"),
            typed(value_length_sql(column.hash, data_type), "Num"),
        ]

    return per_variable


def with_variable_metadata(context: SourceContext) -> str:
    if wants_variable_length_summary(context.rule):
        # rule only wants max length per variable
        # no point unpivoting every value for that
        return variable_length_summary(context, [(context.dataset_metadata, context.rule_dataset())], False)
    columns = columns_of(VALUES_WITH_VARIABLE_METADATA)
    per_variable = variable_value_metadata(context.dataset_metadata.variables)
    return _unpivot_view(context, _source_schema(context), columns, per_variable, "Num")


def with_dataset_metadata(context: SourceContext) -> str:
    source_schema = _source_schema(context)
    dataset = context.dataset_metadata
    record_count = context.store.record_count(context.rule_dataset())
    columns = columns_of(
        {
            "row_number": "Num",
            "variable_name": "Char",
            "variable_value": "Char",
            "dataset_location": "Char",
            "dataset_name": "Char",
            "dataset_label": "Char",
            "record_count": "Num",
        }
    )
    # same dataset-level constants on every row
    constants = [
        typed(literal(dataset.filename), "Char"),
        typed(literal(dataset.name), "Char"),
        typed(literal(dataset.label or ""), "Char"),
        typed(literal(record_count), "Num"),
    ]
    return _unpivot_view(context, source_schema, columns, lambda name, column: constants, "Num")


# --- against define / library ---------------------------------------------------------------


def _define_literal(value, column_type: str) -> str:
    # str(None) is "None", so the stringly "none"/"null"/empty-quotes spellings all mean missing
    text = str(value).replace("'", "''").strip()
    if text.lower() in ["", "none", "null", "''", '""']:
        return typed("NULL", column_type)
    return typed(f"'{text}'", column_type)


def against_define_variables(context: SourceContext) -> str:
    source_schema = context.pgi.schema.get_table(context.rule_dataset())
    define = {
        v.get("define_variable_name", "").upper(): v for v in context.store.define_variables(context.dataset_metadata)
    }
    columns = columns_of(
        {"row_number": "Num", "variable_name": "Char", "variable_value": "Char", **DEFINE_VARIABLES_TYPE}
    )

    def per_variable(name: str, column: SqlColumnSchema) -> List[str]:
        variable = define.get(name.upper(), {})
        return [_define_literal(variable.get(key, ""), key_type) for key, key_type in DEFINE_VARIABLES_TYPE.items()]

    return _unpivot_view(context, source_schema, columns, per_variable, "Char")


def _library_literal(value, column_type: str) -> str:
    """
    Unlike the define, text is written exactly as str() gives it
    A missing label really is the string 'None'.
    Numbers do get NULL.
    TODO: This is kinda inconsistent with the define literal handling.
    """
    text = str(value)
    if column_type == "Char":
        return typed(literal(text), "Char")
    if not text or text == "None":
        return typed("NULL", column_type)
    return typed(text, column_type)


def against_library(context: SourceContext) -> str:
    source_schema = context.pgi.schema.get_table(context.rule_dataset())
    library = {
        v.get("library_variable_name", "").upper(): v for v in context.store.library_variables(context.dataset_metadata)
    }
    codelists = _codelist_lookups(
        context, {str(v.get("library_variable_ccode", "")) for v in library.values() if v is not None}
    )
    columns = columns_of(
        {
            "row_number": "Num",
            "variable_name": "Char",
            "variable_value": "Char",
            **LIBRARY_VARIABLES_TYPE,
            "library_variable_ccode_values": "Char",
            "library_variable_codelist_name": "Char",
            "library_variable_codelist_value": "Char",
        }
    )

    def per_variable(name: str, column: SqlColumnSchema) -> List[str]:
        variable = library.get(name.upper(), {})
        cells = [_library_literal(variable.get(key, ""), key_type) for key, key_type in LIBRARY_VARIABLES_TYPE.items()]
        ccode = str(variable.get("library_variable_ccode", ""))
        values, codelist_name, codelist_value = codelists.get(ccode, (None, None, None))
        return [
            *cells,
            typed(literal(values), "Char"),
            typed(literal(codelist_name), "Char"),
            typed(literal(codelist_value), "Char"),
        ]

    return _unpivot_view(context, source_schema, columns, per_variable, "Char")


def _codelist_lookups(context: SourceContext, ccodes: set) -> Dict[str, tuple]:
    """
    ccode -> (its terms as a text array, name, value).
    Two queries total, instead of per variable.
    """
    ccodes = {ccode for ccode in ccodes if ccode}
    if not ccodes:
        return {}
    table = StaticTables.IG_CODELIST_TABLE_NAME.value
    pgi = context.pgi

    pgi.execute_sql(f"SELECT to_regclass('{table}') IS NOT NULL AS loaded")
    if not (pgi.fetch_one() or {}).get("loaded"):
        # no CT was loaded = there's nothing to look terms up in
        return {}

    in_list = ", ".join(literal(ccode) for ccode in sorted(ccodes))
    pgi.execute_sql(
        f"SELECT codelist_code, CAST(ARRAY_AGG(value) AS TEXT) AS terms FROM {table} "
        f"WHERE codelist_code <> '' AND codelist_code IN ({in_list}) GROUP BY codelist_code"
    )
    terms = {row["codelist_code"]: row["terms"] for row in pgi.fetch_all()}
    pgi.execute_sql(
        f"SELECT DISTINCT ON (item_code) item_code, name, value FROM {table} WHERE item_code IN ({in_list})"
    )
    entries = {row["item_code"]: (row["name"], row["value"]) for row in pgi.fetch_all()}
    return {
        ccode: (terms.get(ccode), *(entries.get(ccode, (None, None))))
        for ccode in ccodes
        if ccode in terms or ccode in entries
    }
