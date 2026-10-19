from typing import Dict, Iterable, Tuple

from cdisc_rules_engine.data_service.postgresql_data_service import (
    BaseDatasetMetadata,
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_operations.variable_length_operations import (
    VARIABLE_LENGTH_OPERATIONS,
    VARIABLE_MAX_VALUE_LENGTH,
)

NOT_DATA_COLUMNS = ("id", "source_ds", "source_row_number")


def wants_variable_length_summary(rule: dict) -> bool:
    """Whether a rule reads the per-variable lengths."""
    return any(
        operation.get("operator") in VARIABLE_LENGTH_OPERATIONS for operation in (rule or {}).get("operations") or []
    )


def build_variable_length_summary(
    data_service: PostgresQLDataService,
    table_name: str,
    datasets: Iterable[Tuple[BaseDatasetMetadata, str]],
    with_dataset_name: bool,
) -> str:
    """
    A table of one row per variable of each dataset.
    """
    pgi = data_service.pgi
    schema = SqlTableSchema.derived(table_name, pgi)
    if with_dataset_name:
        schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
    schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
    schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
    schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
    schema.add_column(SqlColumnSchema.generated("variable_length", "Num"))
    schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
    schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))
    schema.add_column(SqlColumnSchema.generated(VARIABLE_MAX_VALUE_LENGTH, "Num"))
    pgi.create_table(schema)

    rows = []
    for dataset_metadata, source_table_id in datasets:
        source_schema = pgi.schema.get_table(source_table_id)
        if not source_schema:
            raise ValueError(f"Table {source_table_id} not found when building the variable lengths of a dataset.")

        columns = [
            name
            for name, column in source_schema.get_columns()
            if name.lower() not in NOT_DATA_COLUMNS and column.origin == "data"
        ]
        if not columns:
            continue
        record_count, longest_values = _longest_values(data_service, source_schema, columns)
        if record_count == 0:
            continue

        declared = {variable.name.upper(): variable for variable in dataset_metadata.variables}
        for name in columns:
            variable = declared.get(name.upper())
            row = {
                "variable_name": name.upper(),
                "variable_label": (variable.label if variable else None) or "",
                "variable_data_type": (variable.type if variable else None) or "",
                "variable_length": (variable.length if variable else None) or 0,
                "variable_order_number": (variable.order if variable else None) or 0,
                "variable_format": (variable.format if variable else None) or "",
                VARIABLE_MAX_VALUE_LENGTH: longest_values[name],
            }
            if with_dataset_name:
                row["dataset_name"] = dataset_metadata.name
            rows.append(row)

    pgi.insert_data(table_name, rows)
    return table_name


def _longest_values(data_service: PostgresQLDataService, source_schema: SqlTableSchema, columns) -> Tuple[int, Dict]:
    lengths = ", ".join(
        f"MAX(LENGTH(CAST({source_schema.get_column_hash(name)} AS TEXT))) AS length_{index}"
        for index, name in enumerate(columns)
    )
    data_service.pgi.execute_sql(f"SELECT COUNT(*) AS records, {lengths} FROM {source_schema.hash}")
    row = data_service.pgi.fetch_one()
    return row["records"], {name: row[f"length_{index}"] for index, name in enumerate(columns)}
