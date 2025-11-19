from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlValueCheckWithVariableMetadataBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Value Check with Variable Metadata rules.
    Converts dataset from wide to long format (unpivots) and attaches variable metadata.

    Example table structure:
       row_num | var_name | var_val | var_label | var_data_type | var_size | var_order_num | var_format | var_val_len
    -----------|----------|---------|-----------|---------------|----------|---------------|------------|------------
       1       | STUDYID  | ABC123  | Study ID  | text          | 16       | 1             | $16.       | 6
       1       | USUBJID  | 001     | Subject ID| text          | 20       | 2             | $20.       | 3
       2       | STUDYID  | ABC123  | Study ID  | text          | 16       | 1             | $16.       | 6
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_values_with_variable_metadata"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )

        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        if not source_schema:
            raise ValueError(f"Source table {source_table_id} not found")

        var_metadata = {
            var.name.upper(): {
                "label": var.label or "",
                "data_type": var.type or "",
                "size": var.length or 0,
                "order": var.order or 0,
                "format": var.format or "",
            }
            for var in self.dataset_metadata.variables
        }

        schema = SqlTableSchema.static(table_name)
        schema.add_column(SqlColumnSchema.generated("row_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value_length", "Num"))

        self.data_service.pgi.create_table(schema)

        # build UNPIVOT SQL query with variable metadata (postgresql doesn't have UNPIVOT so we use UNION ALL)
        columns_list = source_schema.get_columns()
        column_names = [name for name, _ in columns_list if name != "id"]  # skip id column

        if column_names:
            select_statements = []
            for col_name in column_names:
                col_hash = source_schema.get_column_hash(col_name)
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
                        '{col_name.upper()}' as variable_name,
                        CAST({col_hash} AS TEXT) as variable_value,
                        '{var_label}' as variable_label,
                        '{var_data_type}' as variable_data_type,
                        {var_size} as variable_size,
                        {var_order} as variable_order_number,
                        '{var_format}' as variable_format,
                        {length_expr} as variable_value_length
                    FROM {source_table_id}
                """
                )

            unpivot_query = " UNION ALL ".join(select_statements)

            insert_query = f"""
                INSERT INTO {schema.hash}
                ({schema.get_column_hash("row_number")},
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
