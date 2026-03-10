from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    LIBRARY_VARIABLES_TYPE,
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

        library_vars = self.get_library_vars()
        library_vars_by_name = {var["library_variable_name"].upper(): var for var in library_vars}

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)

        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))
        for col, type in LIBRARY_VARIABLES_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)

        rows = []

        for variable in self.dataset_metadata.variables:
            var_name = variable.name.upper()
            row = {
                "variable_name": var_name,
                "variable_order_number": variable.order,
                "variable_label": variable.label or "",
                "variable_size": variable.length,
                "variable_data_type": variable.type or "",
                "variable_format": variable.format or "",
            }
            row.update(library_vars_by_name.get(var_name, {}))
            rows.append(row)

        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name
