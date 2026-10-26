"""Rule types that check the dataset's own records."""

from typing import Dict, Tuple

from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.views import ensure_view, literal, typed
from cdisc_rules_engine.metadata.store import DEFINE_DATASETS_TYPE
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema


def record_data(context: SourceContext) -> str:
    # nothing to build
    return context.rule_dataset()


def contents_against_define(context: SourceContext) -> str:
    """
    The dataset's records, each carrying its dataset's metadata and the define.xml's
    metadata for it PLUS a flag for whether the define key variables identify the record.
    """
    table_id = context.rule_dataset()
    pgi = context.pgi
    source = pgi.schema.get_table(table_id)
    if not source:
        raise ValueError(f"Table {table_id} not found")
    dataset = context.dataset_metadata
    define_dataset = context.store.define_dataset(dataset)

    # only add what the dataset doesn't already have.
    # A real column called e.g. dataset_name wins over the constant.
    added: Dict[str, str] = {}
    for column in ["dataset_location", "dataset_name", "dataset_label", "dataset_domain"]:
        if not source.get_column(column):
            added[column] = "Char"
    for column, column_type in DEFINE_DATASETS_TYPE.items():
        if not source.get_column(column):
            added[column] = column_type

    values = {
        "dataset_location": dataset.filename,
        "dataset_name": dataset.name,
        "dataset_label": dataset.label,
        "dataset_domain": dataset.domain,
        **{key: value for key, value in define_dataset.items() if key in DEFINE_DATASETS_TYPE},
    }

    columns = [column for _, column in source.get_columns() if column.name != "id"]
    select = ["t.*"]
    for name, column_type in added.items():
        column = SqlColumnSchema.define(name, column_type)
        columns.append(column)
        select.append(f"{typed(literal(values.get(name)), column_type)} AS {column.hash}")

    unique = SqlColumnSchema.define("define_key_sequence_is_unique", "Bool")
    columns.append(unique)
    unique_sql, joins, pivot = _key_sequence_is_unique(context, table_id, define_dataset)
    select.append(f"{unique_sql} AS {unique.hash}")

    body = f"{pivot}SELECT {', '.join(select)} FROM {source.hash} t {joins}".strip()
    return ensure_view(pgi, body, columns, source="derived")


def _key_sequence_is_unique(context: SourceContext, table_id: str, define_dataset: dict) -> Tuple[str, str, str]:
    """
    Returns (expression, joins, cte) for "do this record's define.xml keys identify it".
    The awkward part is SUPP, yuck.
    A key can be a QNAM, which lives in SUPPxx rather than on the record.
    In that case SUPP is pivoted to one JSONB per (USUBJID, IDVAR, IDVARVAL) and joined
    back on each IDVAR, and that jsonb becomes part of the partition key.
    """
    pgi = context.pgi
    source = pgi.schema.get_table(table_id)
    key_sequence = [col for col in (define_dataset.get("define_dataset_key_sequence") or "").split(",") if col]
    if not key_sequence:
        # no keys in the define so therefore nothing to be duplicated on
        return "true", "", ""

    supp = pgi.schema.get_table(f"SUPP{table_id}".lower())
    if not supp:
        return _unique_over([_column(source, "t", col) for col in key_sequence]), "", ""

    pgi.execute_sql(
        f"SELECT STRING_AGG(DISTINCT {_column(supp, None, 'IDVAR')}, ', ') as idvars, "
        f"STRING_AGG(DISTINCT {_column(supp, None, 'QNAM')}, ', ') as qnams from {supp.hash};"
    )
    supp_columns = pgi.fetch_one() or {}
    # a blank IDVAR (e.g. in SUPPDM) links the SUPP record to the subject rather than to a record
    idvar_cols = (supp_columns.get("idvars") or "").split(", ")
    qnam_cols = (supp_columns.get("qnams") or "").split(", ")
    parent_keys = [col for col in key_sequence if col not in qnam_cols]

    supp_key = ", ".join(
        f"{_column(supp, None, col)} AS {col}" for col in ("STUDYID", "RDOMAIN", "USUBJID", "IDVARVAL", "IDVAR")
    )
    pivot = f"""WITH pivoted_supp as (
            SELECT
                {supp_key},
                jsonb_object_agg({_column(supp, None, "QNAM")}, {_column(supp, None, "QVAL")}) AS all_supp_vars
            FROM {supp.hash}
            GROUP BY 1, 2, 3, 4, 5
        ) """
    joins = "\n".join(supp_join(source, index, idvar) for index, idvar in enumerate(idvar_cols))
    # a record normally only matches SUPP through one of its IDVARs
    # take whichever join hit
    supp_values = f"COALESCE({', '.join(f'ps_{index}.all_supp_vars' for index in range(len(idvar_cols)))})"
    keys = [*(_column(source, "t", col) for col in parent_keys), supp_values]
    return _unique_over(keys), joins, pivot


def supp_join(source, index: int, idvar: str) -> str:
    """The SUPP records of a record: those for its IDVAR value, or for its subject when IDVAR is blank."""
    join = f"""LEFT JOIN pivoted_supp ps_{index}
            ON {_column(source, "t", "STUDYID")} = ps_{index}.STUDYID
            AND {_column(source, "t", "DOMAIN")} = ps_{index}.RDOMAIN
            AND {_column(source, "t", "USUBJID")} = ps_{index}.USUBJID"""
    if not idvar:
        return f"{join}\n            AND COALESCE(ps_{index}.IDVAR, '') = ''"
    escaped = idvar.replace("'", "''")
    return f"""{join}
            AND ps_{index}.IDVAR = '{escaped}'
            AND {_column(source, "t", idvar)}::text = ps_{index}.IDVARVAL::text"""


def _unique_over(keys) -> str:
    return f"(COUNT(*) OVER (PARTITION BY {', '.join(keys)}) = 1)"


def _column(schema, alias, name: str) -> str:
    """
    A column by its variable name: its hash in the table (which differs from the name for
    reserved words), or the name as written when the table doesn't have it.
    """
    column_hash = schema.get_column_hash(name) if schema is not None else None
    reference = column_hash or name
    return f"{alias}.{reference}" if alias else reference
