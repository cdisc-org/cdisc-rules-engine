"""Rule types with one row per variable: the dataset's, the define.xml's, or the library's."""

from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.views import rows_view
from cdisc_rules_engine.metadata.store import DEFINE_VARIABLES_TYPE, LIBRARY_VARIABLES_TYPE

VARIABLE_COLUMNS = {
    "variable_name": "Char",
    "variable_order_number": "Num",
    "variable_label": "Char",
    "variable_length": "Num",
    "variable_data_type": "Char",
    "variable_format": "Char",
}

NO_DEFINE_VARIABLE = {key: None for key in DEFINE_VARIABLES_TYPE}
NO_LIBRARY_VARIABLE = {key: None for key in LIBRARY_VARIABLES_TYPE}


def _by_name(variables: list, key: str) -> dict:
    return {variable.get(key, "").upper(): variable for variable in variables}


# NOTE: the three "dataset variable" builders below differ in how they default missing
# metadata (None vs "" vs 0).
# That's deliberate btw. I know it doesn't look deliberate but it is.
# Each matches the dataset builder it replaced, and rules were written against those outputs.
# Don't tidy them into one helper without running the rule regression pls.


def metadata(context: SourceContext) -> str:
    rows = [
        {
            "variable_name": variable.name.upper(),
            "variable_order_number": variable.order,
            "variable_label": variable.label,
            "variable_length": variable.length,
            "variable_data_type": variable.type,
            "variable_format": variable.format,
        }
        for variable in context.dataset_metadata.variables
    ]
    return rows_view(context, VARIABLE_COLUMNS, rows)


def against_library(context: SourceContext) -> str:
    library = {
        var["library_variable_name"].upper(): var for var in context.store.library_variables(context.dataset_metadata)
    }
    rows = []
    for variable in context.dataset_metadata.variables:
        name = variable.name.upper()
        rows.append(
            {
                "variable_name": name,
                "variable_order_number": variable.order,
                "variable_label": variable.label or "",
                "variable_length": variable.length,
                "variable_data_type": variable.type or "",
                "variable_format": variable.format or "",
                **library.get(name, NO_LIBRARY_VARIABLE),
            }
        )
    return rows_view(context, {**VARIABLE_COLUMNS, **LIBRARY_VARIABLES_TYPE}, rows)


def against_define(context: SourceContext) -> str:
    define = _by_name(context.store.define_variables(context.dataset_metadata), "define_variable_name")
    rows = []
    for variable in context.dataset_metadata.variables:
        name = variable.name.upper()
        rows.append(
            {
                "variable_name": name,
                "variable_order_number": variable.order,
                "variable_label": variable.label or "",
                "variable_length": variable.length or 0,
                "variable_data_type": variable.type or "",
                "variable_format": variable.format or "",
                **define.get(name, NO_DEFINE_VARIABLE),
            }
        )
    return rows_view(context, {**VARIABLE_COLUMNS, **DEFINE_VARIABLES_TYPE}, rows)


def against_define_and_library(context: SourceContext) -> str:
    """
    Union of the define.xml's variables and the dataset's.
    Define order first, then anything the dataset has that the define doesn't mention.
    Each row gets data, define and library metadata, plus how populated the variable actually is.
    """
    dataset = context.dataset_metadata
    define = _by_name(context.store.define_variables(dataset), "define_variable_name")
    library = {var["library_variable_name"].upper(): var for var in context.store.library_variables(dataset)}
    declared = {variable.name.upper(): variable for variable in dataset.variables}
    names = list(define) + [name for name in declared if name not in define]

    populated = _populated_counts(context, context.rule_dataset(), dataset.variables)

    rows = []
    for name in names:
        variable = declared.get(name)
        count = populated["counts"].get(name, 0) if variable else 0
        has_empty = populated["table_is_empty"] or not variable or not populated["has_column"].get(name) or count == 0
        row = {
            "variable_name": name if variable else "",
            "variable_order_number": variable.order if variable else 0,
            "variable_label": variable.label if variable else "",
            "variable_length": variable.length if variable else 0,
            "variable_data_type": variable.type if variable else "",
            # stored as the bool string (i.e. "True"/"False")
            "variable_has_empty_values": str(has_empty),
            "variable_count": count,
            "variable_is_empty": count == 0,
        }
        row.update(define.get(name, NO_DEFINE_VARIABLE))
        row.update(library.get(name, NO_LIBRARY_VARIABLE))
        rows.append(row)

    columns = {
        "variable_name": "Char",
        "variable_order_number": "Num",
        "variable_label": "Char",
        "variable_length": "Num",
        "variable_data_type": "Char",
        "variable_has_empty_values": "Bool",
        "variable_count": "Num",
        "variable_is_empty": "Bool",
        **DEFINE_VARIABLES_TYPE,
        **LIBRARY_VARIABLES_TYPE,
    }
    return rows_view(context, columns, rows)


def _populated_counts(context: SourceContext, table_id: str, variables) -> dict:
    """How many records have a non-blank value per variable."""
    pgi = context.pgi
    table_hash = pgi.schema.get_table_hash(table_id)
    hashes = {}
    for variable in variables:
        column_hash = pgi.schema.get_column_hash(table_id, variable.name.upper())
        if column_hash:
            hashes[variable.name.upper()] = column_hash

    # Old builder ran two COUNT queries per variable.
    # One FILTERed COUNT per column gets the same numbers from a single scan, which matters a lot on wide datasets.
    # TODO: Maybe we could optimise further by combining more operations?
    # But this already reduces the number of scans significantly.
    counts_sql = [
        f"COUNT(*) FILTER (WHERE TRIM(COALESCE(CAST({column_hash} AS TEXT), '')) != '') AS c{index}"
        for index, column_hash in enumerate(hashes.values())
    ]
    pgi.execute_sql(f"SELECT {', '.join(['COUNT(*) AS records', *counts_sql])} FROM {table_hash}")
    row = pgi.fetch_one() or {}
    return {
        "table_is_empty": not row.get("records"),
        "has_column": {name: True for name in hashes},
        "counts": {name: row.get(f"c{index}") or 0 for index, name in enumerate(hashes)},
    }


def define_items(context: SourceContext) -> str:
    return rows_view(context, dict(DEFINE_VARIABLES_TYPE), context.store.define_variables(context.dataset_metadata))


def define_items_against_library(context: SourceContext) -> str:
    """Every variable named by either the define or the library, sorted, with both sides' metadata."""
    dataset = context.dataset_metadata
    define = {var["define_variable_name"].upper(): var for var in context.store.define_variables(dataset)}
    library = {var["library_variable_name"].upper(): var for var in context.store.library_variables(dataset)}
    rows = [
        {**define.get(name, NO_DEFINE_VARIABLE), **library.get(name, NO_LIBRARY_VARIABLE)}
        for name in sorted(set(define) | set(library))
    ]
    return rows_view(context, {**DEFINE_VARIABLES_TYPE, **LIBRARY_VARIABLES_TYPE}, rows)
