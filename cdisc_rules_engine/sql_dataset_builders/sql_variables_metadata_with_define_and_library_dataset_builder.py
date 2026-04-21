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

        # adjust to define first check (ie vars in define but NOT in dataset will get included - note that therefore vars in dataset but not in define WILL NOT be included) # noqa
        for define_var, _ in define_vars_by_name.items():
            define_var_name = define_var.upper() if define_var else ""
            var = next((v for v in self.dataset_metadata.variables if v.name.upper() == define_var_name), None)
            has_empty = self._has_empty_values(source_table_id, source_table_hash, var, table_is_empty)
            var_count = self._value_count(source_table_id, source_table_hash, var) if var else 0
            row = {
                "variable_name": var.name.upper() if var else "",
                "variable_order_number": var.order if var else 0,
                "variable_label": var.label if var else "",
                "variable_size": var.length if var else 0,
                "variable_data_type": var.type if var else "",
                "variable_has_empty_values": str(has_empty),
                "variable_count": var_count,
                "variable_is_empty": True if var_count == 0 else False,
            }
            row.update(define_vars_by_name.get(define_var_name, {k: None for k in DEFINE_VARIABLES_TYPE}))
            row.update(library_vars_by_name.get(define_var_name, {k: None for k in LIBRARY_VARIABLES_TYPE}))

            # leaving previous implementation of dataset first as a comment for now
            # for var in self.dataset_metadata.variables:
            #     var_name = var.name.upper() if var else ""
            #     has_empty = self._has_empty_values(source_table_id, source_table_hash, var, table_is_empty)
            #     var_count = self._value_count(source_table_id, source_table_hash, var)

            #     row = {
            #         "variable_name": var.name.upper() if var else "",
            #         "variable_order_number": var.order if var else 0,
            #         "variable_label": var.label if var else "",
            #         "variable_size": var.length if var else 0,
            #         "variable_data_type": var.type if var else "",
            #         "variable_has_empty_values": str(has_empty),
            #         "variable_count": var_count,
            #         "variable_is_empty": True if var_count == 0 else False,
            #     }
            #     row.update(define_vars_by_name.get(var_name, {k: None for k in DEFINE_VARIABLES_TYPE}))
            #     row.update(library_vars_by_name.get(var_name, {k: None for k in LIBRARY_VARIABLES_TYPE}))

            rows.append(row)

            schema = SqlTableSchema.derived(table_name, self.data_service.pgi)

            schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
            schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
            schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
            schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
            schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
            schema.add_column(SqlColumnSchema.generated("variable_has_empty_values", "Bool"))
            schema.add_column(SqlColumnSchema.generated("variable_count", "Num"))
            schema.add_column(SqlColumnSchema.generated("variable_is_empty", "Bool"))
            for col, type in list(DEFINE_VARIABLES_TYPE.items()) + list(LIBRARY_VARIABLES_TYPE.items()):
                schema.add_column(SqlColumnSchema.generated(col, type))

            self.data_service.pgi.create_table(schema)
        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name
