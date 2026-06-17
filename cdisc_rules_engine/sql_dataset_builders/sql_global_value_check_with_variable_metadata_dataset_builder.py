from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder


class SqlGlobalValueCheckwithVariableMetadataDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Global Value Check with Variable Metadata rules.
    Converts all datasets from wide to long format (unpivots) and attaches variable metadata.
    """

    def build(self) -> str:
        table_name = "global_value_check"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        all_ds_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("row_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value_length", "Num"))

        self.data_service.pgi.create_table(schema)

        for ds_metadata in all_ds_metadata:
            ds_table_hash = self.data_service.pgi.schema.get_table_hash(ds_metadata.name)
            if not ds_table_hash:
                raise ValueError(f"Table {ds_metadata.name} not found when building global value check dataset.")

            ds_schema = self.data_service.pgi.schema.get_table(ds_metadata.name)
            if not ds_schema:
                raise ValueError(f"Table {ds_metadata.name} not found when building global value check dataset.")

            var_metadata = {
                var.name.upper(): {
                    "label": var.label or "",
                    "data_type": var.type or "",
                    "size": var.length or 0,
                    "order": var.order or 0,
                    "format": var.format or "",
                }
                for var in ds_metadata.variables
            }

            columns_list = ds_schema.get_columns()
            column_names = [
                name
                for name, schema in columns_list
                if name.lower() not in ["id", "source_ds", "source_row_number"] and schema.origin == "data"
            ]

            if column_names:
                select_statements = []
                for col_name in column_names:
                    col_hash = ds_schema.get_column_hash(col_name)
                    col_upper = col_name.upper()
                    metadata = var_metadata.get(col_upper, {})

                    var_label = metadata.get("label", "")
                    var_data_type = metadata.get("data_type", "")
                    var_size = metadata.get("size", 0)
                    var_order = metadata.get("order", 0)
                    var_format = metadata.get("format", "")

                    # calculate variable_value_length
                    if var_data_type == "integer":
                        # length of the value without leading zeros
                        length_expr = f"LENGTH(LTRIM(CAST({col_hash} AS TEXT), '0'))"
                    elif var_data_type == "float":
                        # length without leading zeros and decimal point
                        length_expr = f"LENGTH(REPLACE(LTRIM(CAST({col_hash} AS TEXT), '0'), '.', ''))"
                    else:  # text or default
                        # just the length of the string
                        length_expr = f"LENGTH(CAST({col_hash} AS TEXT))"

                    select_statements.append(
                        f"""
                        SELECT
                            ROW_NUMBER() OVER () as row_number,
                            '{ds_metadata.name}' as dataset_name,
                            '{col_name.upper()}' as variable_name,
                            CAST({col_hash} AS TEXT) as variable_value,
                            '{var_label}' as variable_label,
                            '{var_data_type}' as variable_data_type,
                            {var_size} as variable_size,
                            {var_order} as variable_order_number,
                            '{var_format}' as variable_format,
                            {length_expr} as variable_value_length
                        FROM {ds_table_hash}
                    """
                    )

                unpivot_query = " UNION ALL ".join(select_statements)

                insert_query = f"""
                    INSERT INTO {schema.hash}
                    ({schema.get_column_hash("row_number")},
                    {schema.get_column_hash("dataset_name")},
                    {schema.get_column_hash("variable_name")},
                    {schema.get_column_hash("variable_value")},
                    {schema.get_column_hash("variable_label")},
                    {schema.get_column_hash("variable_data_type")},
                    {schema.get_column_hash("variable_size")},
                    {schema.get_column_hash("variable_order_number")},
                    {schema.get_column_hash("variable_format")},
                    {schema.get_column_hash("variable_value_length")})
                    {unpivot_query};
                """

                self.data_service.pgi.execute_sql(insert_query)

        return table_name
