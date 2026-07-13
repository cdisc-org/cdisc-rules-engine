from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    LIBRARY_VARIABLES_TYPE,
)
from cdisc_rules_engine.enums.static_tables import StaticTables


class SqlValueCheckAgainstLibraryDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Returns a long dataset (unpivoted) where each value in each row of the original dataset
    is a row in the new dataset. The library variable metadata corresponding to each row's
    variable is attached.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_value_check_library_variables"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )
        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)

        library_vars = self.get_library_vars()
        library_vars_by_name = {v.get("library_variable_name", "").upper(): v for v in library_vars}

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("row_number", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        for col, type in LIBRARY_VARIABLES_TYPE.items():
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
            for col_name in ["row_number", "variable_name", "variable_value"] + list(LIBRARY_VARIABLES_TYPE.keys())
        }
        for col_name in column_names:
            col_hash = col_hash_map[col_name]
            col_upper = col_name.upper()
            l_var = library_vars_by_name.get(col_upper, {})

            select_parts = [
                f"CAST(ROW_NUMBER() OVER (ORDER BY id) AS TEXT) as {new_col_hash_map['row_number']}",
                f"'{col_upper}' as {new_col_hash_map['variable_name']}",
                f"CAST({col_hash} AS TEXT) as {new_col_hash_map['variable_value']}",
            ]

            for key in LIBRARY_VARIABLES_TYPE.keys():
                type = self.data_service.pgi.schema.get_column(table_name, key).type
                val = str(l_var.get(key, ""))
                query_val = f"'{val}'" if type == "Char" else val
                query_val = query_val if query_val else "NULL"
                select_parts.append(f"{query_val} as {new_col_hash_map[key]}")

            select_statements.append(f"SELECT {', '.join(select_parts)} FROM {source_table_hash}")

        target_columns = [
            new_col_hash_map["row_number"],
            new_col_hash_map["variable_name"],
            new_col_hash_map["variable_value"],
        ] + [new_col_hash_map[key] for key in LIBRARY_VARIABLES_TYPE.keys()]

        columns_clause = ", ".join(target_columns)

        unpivot_query = " UNION ALL ".join(select_statements)
        insert_query = f"INSERT INTO {schema.hash} ({columns_clause}) {unpivot_query};"

        self.data_service.pgi.execute_sql(insert_query)

        self.data_service.pgi.add_column(table_name, SqlColumnSchema.define("library_variable_ccode_values", "Char"))
        ccode_vals_col_hash = self.data_service.pgi.schema.get_column_hash(table_name, "library_variable_ccode_values")
        codelist_query = f"""
            UPDATE {schema.hash} t
            SET {ccode_vals_col_hash} = sub.library_variable_ccode_values
            FROM (
                WITH t1 AS (SELECT codelist_code, ARRAY_AGG(value) AS library_variable_ccode_values
                FROM {StaticTables.IG_CODELIST_TABLE_NAME.value}
                WHERE codelist_code <> ''
                GROUP BY codelist_code)
                SELECT a.*, b.library_variable_ccode_values
                FROM {schema.hash} a
                JOIN t1 b
                ON a.{new_col_hash_map['library_variable_ccode']} = b.codelist_code
            ) sub
            WHERE t.id = sub.id;
        """

        self.data_service.pgi.execute_sql(codelist_query)

        self.data_service.pgi.add_column(table_name, SqlColumnSchema.define("library_variable_codelist_name", "Char"))
        codelist_name_col_hash = self.data_service.pgi.schema.get_column_hash(
            table_name, "library_variable_codelist_name"
        )
        codelist_name_query = f"""
            UPDATE {schema.hash} t
            SET {codelist_name_col_hash} = sub.library_variable_codelist_name
            FROM (
                SELECT item_code, name as library_variable_codelist_name
                FROM {StaticTables.IG_CODELIST_TABLE_NAME.value}
            ) sub
            WHERE t.{new_col_hash_map['library_variable_ccode']} = sub.item_code;
        """

        self.data_service.pgi.execute_sql(codelist_name_query)

        return table_name
