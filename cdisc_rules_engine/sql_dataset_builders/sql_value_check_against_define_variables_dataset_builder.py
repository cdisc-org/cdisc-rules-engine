from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VARIABLES_TYPE,
)


class SqlValueCheckAgainstDefineVariablesDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Returns a long dataset (unpivoted) where each value in each row of the original dataset
    is a row in the new dataset. The Define XML variable metadata corresponding to each row's
    variable is attached.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_value_check_define_variables"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )
        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)

        define_vars = self.get_define_vars()
        define_vars_by_name = {v.get("define_variable_name", "").upper(): v for v in define_vars}

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("row_number", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        for col, type in DEFINE_VARIABLES_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)

        columns_list = source_schema.get_columns()
        column_names = [
            name
            for name, sc in columns_list
            if name.lower() not in ["id", "source_ds", "source_row_number"] and sc.origin == "data"
        ]

        select_statements = []
        col_hash_map = {col_name: source_schema.get_column_hash(col_name) for col_name in column_names}
        new_col_hash_map = {
            col_name: schema.get_column_hash(col_name)
            for col_name in ["row_number", "variable_name", "variable_value"] + list(DEFINE_VARIABLES_TYPE.keys())
        }
        for col_name in column_names:
            col_hash = col_hash_map[col_name]
            col_upper = col_name.upper()
            d_var = define_vars_by_name.get(col_upper, {})

            select_parts = [
                f"CAST(ROW_NUMBER() OVER (ORDER BY id) AS TEXT) as {new_col_hash_map['row_number']}",
                f"CAST('{col_upper}' AS TEXT) as {new_col_hash_map['variable_name']}",
                f"CAST({col_hash} AS TEXT) as {new_col_hash_map['variable_value']}",
            ]

            for key in DEFINE_VARIABLES_TYPE.keys():
                val = str(d_var.get(key, ""))
                select_parts.append(f"CAST('{val}' AS TEXT) as {new_col_hash_map[key]}")

            select_statements.append(f"SELECT {', '.join(select_parts)} FROM {source_table_hash}")

        target_columns = [
            new_col_hash_map["row_number"],
            new_col_hash_map["variable_name"],
            new_col_hash_map["variable_value"],
        ] + [new_col_hash_map[key] for key in DEFINE_VARIABLES_TYPE.keys()]

        columns_clause = ", ".join(target_columns)

        unpivot_query = " UNION ALL ".join(select_statements)
        insert_query = f"INSERT INTO {schema.hash} ({columns_clause}) {unpivot_query};"

        self.data_service.pgi.execute_sql(insert_query)

        return table_name
