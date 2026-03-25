from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VARIABLES_TYPE,
    LIBRARY_VARIABLES_TYPE,
)


class SqlVariablesMetadataWithDefineAndLibraryDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Creates a comprehensive variable metadata table that merges physical dataset metadata,
    Define-XML metadata, and Library metadata.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_var_meta_define_library"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        define_vars = self.get_define_vars()
        define_vars_by_name = {v.get("define_variable_name", "").upper(): v for v in define_vars}

        library_vars = self.get_library_vars()
        library_vars_by_name = {var["library_variable_name"].upper(): var for var in library_vars}

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )
        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)
        empty_check_query = f"SELECT * FROM {source_table_hash} LIMIT 1"
        self.data_service.pgi.execute_sql(empty_check_query)
        empty_check_data = self.data_service.pgi.fetch_all()
        table_is_empty = len(empty_check_data) == 0

        rows = []
        for var in self.dataset_metadata.variables:
            var_name = var.name.upper() if var else ""
            has_empty = self._has_empty_values(source_table_id, source_table_hash, var, table_is_empty)

            row = {
                "variable_name": var.name.upper() if var else "",
                "variable_order_number": var.order if var else 0,
                "variable_label": var.label if var else "",
                "variable_size": var.length if var else 0,
                "variable_data_type": var.type if var else "",
                "variable_has_empty_values": str(has_empty),
            }
            row.update(define_vars_by_name.get(var_name, {k: None for k in DEFINE_VARIABLES_TYPE}))
            row.update(library_vars_by_name.get(var_name, {k: None for k in LIBRARY_VARIABLES_TYPE}))

            rows.append(row)

            schema = SqlTableSchema.derived(table_name, self.data_service.pgi)

            schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
            schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
            schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
            schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
            schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
            schema.add_column(SqlColumnSchema.generated("variable_has_empty_values", "Bool"))
            for col, type in list(DEFINE_VARIABLES_TYPE.items()) + list(LIBRARY_VARIABLES_TYPE.items()):
                schema.add_column(SqlColumnSchema.generated(col, type))

            self.data_service.pgi.create_table(schema)
        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name

    def _has_empty_values(self, source_table_id: str, source_table_hash: str, var, table_is_empty: bool) -> bool:
        if table_is_empty or not var:
            return True

        var_name = var.name.upper()
        col_hash = self.data_service.pgi.schema.get_column_hash(source_table_id, var_name)
        if not col_hash:
            return True

        val_query = (
            f"SELECT COUNT(*) as cnt FROM {source_table_hash} "
            f"WHERE TRIM(COALESCE(CAST({col_hash} AS TEXT), '')) != ''"
        )
        self.data_service.pgi.execute_sql(val_query)
        val_res = self.data_service.pgi.fetch_one()

        return not (val_res and val_res["cnt"] > 0)
