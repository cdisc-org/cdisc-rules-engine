from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlVariablesMetadataWithLibraryBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Variable Metadata Check Against Library rules.
    Creates a table that merges variable metadata with library metadata.

    Example table structure (one row per variable):
       var_name | var_label | var_data_type | ... | library_var_name | library_var_label | library_var_core | ...
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_var_metadata_with_library"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        library_metadata = self.standards_context.get_library_variables_metadata(self.dataset_metadata)

        schema = SqlTableSchema.static(table_name)

        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))

        schema.add_column(SqlColumnSchema.generated("library_variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("library_variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("library_variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("library_variable_role", "Char"))
        schema.add_column(SqlColumnSchema.generated("library_variable_core", "Char"))
        schema.add_column(SqlColumnSchema.generated("library_variable_order_number", "Num"))

        self.data_service.pgi.create_table(schema)

        rows = []
        library_vars_by_name = {var["name"]: var for var in library_metadata}

        for variable in self.dataset_metadata.variables:
            var_name = variable.name.upper()
            library_var = library_vars_by_name.get(var_name, {})

            row = {
                "variable_name": var_name,
                "variable_order_number": variable.order,
                "variable_label": variable.label or "",
                "variable_size": variable.length,
                "variable_data_type": variable.type or "",
                "variable_format": variable.format or "",
                "library_variable_name": library_var.get("name", ""),
                "library_variable_label": library_var.get("label", ""),
                "library_variable_data_type": library_var.get("data_type", ""),
                "library_variable_role": library_var.get("role", ""),
                "library_variable_core": library_var.get("core", ""),
                "library_variable_order_number": library_var.get("order_number", 0),
            }
            rows.append(row)

        existing_vars = {var.name.upper() for var in self.dataset_metadata.variables}
        for lib_var in library_metadata:
            if lib_var["name"] not in existing_vars:
                row = {
                    "variable_name": "",
                    "variable_order_number": 0,
                    "variable_label": "",
                    "variable_size": 0,
                    "variable_data_type": "",
                    "variable_format": "",
                    "library_variable_name": lib_var.get("name", ""),
                    "library_variable_label": lib_var.get("label", ""),
                    "library_variable_data_type": lib_var.get("data_type", ""),
                    "library_variable_role": lib_var.get("role", ""),
                    "library_variable_core": lib_var.get("core", ""),
                    "library_variable_order_number": lib_var.get("order_number", 0),
                }
                rows.append(row)

        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name
