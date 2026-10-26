"""
Value Check against Define XML Value Level Metadata (VLM).
A value-level metadata entry applies to a value when the value's record matches the VLM's
where clause.
So for each VLM we SELECT the matching records' value for that variable and UNION ALL the lot.
Each row also carries the rest of its record so a rule can reference sibling variables.
e.g. LBTESTCD alongside LBORRES.
"""

from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.views import columns_of, empty_body, ensure_view, typed
from cdisc_rules_engine.data_service.sql_serialiser import SQLSerialiser
from cdisc_rules_engine.metadata.store import DEFINE_VLM_TYPE
from cdisc_rules_engine.metadata.vlm import UntranslatableWhereClause, where_clause_to_sql
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.services import logger

VLM_SOURCE = "vlm_source"
VLM_ROW_NUMBER = "vlm_source_row_number"

VLM_COLUMNS = {
    "row_number": "Num",
    "variable_name": "Char",
    "variable_value": "Char",
    "define_variable_name": "Char",
    **DEFINE_VLM_TYPE,
}


def _vlm_literal(value, column_type: str) -> str:
    if value is None or str(value).strip() == "":
        return typed("NULL", column_type)
    escaped = str(value).strip().replace("'", "''")
    return typed(f"'{escaped}'", column_type)


def against_define_vlm(context: SourceContext) -> str:
    source_schema = context.pgi.schema.get_table(context.rule_dataset())

    record_columns = []
    for name, column in source_schema.get_columns():
        if name.lower() == "id" or column.origin != "data":
            continue
        if name.lower() in VLM_COLUMNS:
            # e.g. a dataset that literally has a variable_value column.
            # Rare enough that dropping it with a warning beats inventing a renaming scheme, thank god.
            logger.warning(f"Source column '{name}' clashes with a VLM dataset column and is not available")
            continue
        record_columns.append((name, column))
    columns = [*columns_of(VLM_COLUMNS), *[SqlColumnSchema.generated(name, c.type) for name, c in record_columns]]

    selects = []
    for vlm in context.store.define_vlms(context.dataset_metadata):
        variable_name = (vlm.get("define_variable_name") or "").upper()
        value_hash = source_schema.get_column_hash(variable_name)
        if not value_hash:
            logger.warning(f"Skipping VLM for '{variable_name}': variable is not in the dataset")
            continue
        try:
            where = where_clause_to_sql(vlm.get("where_clause"), source_schema, VLM_SOURCE)
        except UntranslatableWhereClause as e:
            # skip the one VLM rather than failing the whole rule
            logger.warning(f"Skipping VLM for '{variable_name}': {e}")
            continue

        safe_name = variable_name.replace("'", "''")
        parts = [
            VLM_ROW_NUMBER,
            f"CAST('{safe_name}' AS TEXT)",
            f"CAST({VLM_SOURCE}.{value_hash} AS TEXT)",
            f"CAST('{safe_name}' AS TEXT)",
            *[_vlm_literal(vlm.get(key), key_type) for key, key_type in DEFINE_VLM_TYPE.items()],
            *[f"{VLM_SOURCE}.{column.hash}" for _, column in record_columns],
        ]
        selects.append(f"SELECT {', '.join(parts)} FROM {VLM_SOURCE} WHERE {where}")

    if not selects:
        return ensure_view(context.pgi, empty_body(columns), columns)

    names = ", ".join(column.hash for column in columns)
    # one more cast on the outside so the view's column types are exactly what the schema says
    typed_columns = ", ".join(
        f"CAST(described.{column.hash} AS {SQLSerialiser.column_type_to_sql_type(column.type)}) AS {column.hash}"
        for column in columns
    )
    # row_number is the record's position in the dataset, numbered once up front so every VLM branch agrees on it
    body = (
        f"WITH {VLM_SOURCE} AS (SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS {VLM_ROW_NUMBER} "
        f"FROM {source_schema.hash}) "
        f"SELECT ROW_NUMBER() OVER () AS id, {typed_columns} "
        f"FROM ({' UNION ALL '.join(selects)}) AS described({names})"
    )
    return ensure_view(context.pgi, body, columns)
