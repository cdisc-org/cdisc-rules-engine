from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VARIABLES_TYPE,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import DefineXMLReaderFactory


class SqlVariablesMetadataWithDefineDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Creates a table merging the physical dataset variable metadata with Define-XML variable metadata.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_var_metadata_with_define"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        define_vars = define_reader.extract_variables_metadata(domain_name=self.dataset_metadata.domain)
        for var in define_vars:
            for k, v in var.items():
                var[k] = ",".join(str(i) for i in v) if isinstance(v, list) else v
        define_vars_by_name = {v.get("define_variable_name", "").upper(): v for v in define_vars}

        rows = []

        for var in self.dataset_metadata.variables:
            var_name = var.name.upper()

            row = {
                "variable_name": var_name,
                "variable_order_number": var.order,
                "variable_label": var.label or "",
                "variable_size": var.length or 0,
                "variable_data_type": var.type or "",
                "variable_format": var.format or "",
            }

            d_var = define_vars_by_name.get(var_name, {k: None for k in DEFINE_VARIABLES_TYPE})
            row.update(d_var)

            rows.append(row)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)

        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))

        for col, type in DEFINE_VARIABLES_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)
        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name
