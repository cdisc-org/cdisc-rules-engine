"""
Variable length summary: one row per variable with the length of its longest value.
Specifically for rules that need to know the maximum/minimum length of variable values.
This avoids scanning every row in a big dataset by using a handful of aggregates instead of every row.
"""

from typing import Dict, Optional, Tuple

from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.views import rows_view, unpivoted_columns
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_operations.variable_length_operations import (
    VARIABLE_LENGTH_OPERATIONS,
    VARIABLE_MAX_VALUE_LENGTH,
)

SUMMARY_COLUMNS = {
    "variable_name": "Char",
    "variable_label": "Char",
    "variable_data_type": "Char",
    "variable_length": "Num",
    "variable_order_number": "Num",
    "variable_format": "Char",
    VARIABLE_MAX_VALUE_LENGTH: "Num",
}


def wants_variable_length_summary(rule: dict) -> bool:
    operations = (rule or {}).get("operations") or []
    return any(operation.get("operator") in VARIABLE_LENGTH_OPERATIONS for operation in operations)


def variable_length_summary(
    context: SourceContext, datasets, with_dataset_name: bool, name: Optional[str] = None
) -> str:
    """
    datasets is a list of (dataset metadata, table id) pairs.
    Datasets with no records are left out entirely, since there's no "longest value" to speak of.
    """
    pgi = context.pgi
    column_types = {"dataset_name": "Char", **SUMMARY_COLUMNS} if with_dataset_name else dict(SUMMARY_COLUMNS)

    rows = []
    for dataset_metadata, source_table_id in datasets:
        source_schema = pgi.schema.get_table(source_table_id)
        if not source_schema:
            raise ValueError(f"Table {source_table_id} not found when building the variable lengths of a dataset.")
        variables = [variable_name for variable_name, _ in unpivoted_columns(source_schema)]
        if not variables:
            continue
        record_count, longest = _longest_values(pgi, source_schema, variables)
        if record_count == 0:
            continue

        declared = {variable.name.upper(): variable for variable in dataset_metadata.variables}
        for variable_name in variables:
            # A column can exist in the data without being in the dataset's metadata
            # still report it but just with blank metadata
            variable = declared.get(variable_name.upper())
            row = {
                "variable_name": variable_name.upper(),
                "variable_label": (variable.label if variable else None) or "",
                "variable_data_type": (variable.type if variable else None) or "",
                "variable_length": (variable.length if variable else None) or 0,
                "variable_order_number": (variable.order if variable else None) or 0,
                "variable_format": (variable.format if variable else None) or "",
                VARIABLE_MAX_VALUE_LENGTH: longest[variable_name],
            }
            if with_dataset_name:
                row["dataset_name"] = dataset_metadata.name
            rows.append(row)
    return rows_view(context, column_types, rows, name=name)


def _longest_values(pgi, source_schema: SqlTableSchema, columns) -> Tuple[int, Dict]:
    lengths = ", ".join(
        f"MAX(LENGTH(CAST({source_schema.get_column_hash(name)} AS TEXT))) AS length_{index}"
        for index, name in enumerate(columns)
    )
    pgi.execute_sql(f"SELECT COUNT(*) AS records, {lengths} FROM {source_schema.hash}")
    row = pgi.fetch_one()
    return row["records"], {name: row[f"length_{index}"] for index, name in enumerate(columns)}
