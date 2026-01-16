from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlValueCheckWithDatasetMetadataBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Value Check with Dataset Metadata rules.
    Converts dataset from wide to long format (unpivots) and attaches dataset metadata.

    Example table structure:
       row_number | variable_name | variable_value | dataset_location | dataset_name | dataset_label
    --------------|---------------|----------------|------------------|--------------|---------------
       1          | STUDYID       | ABC123         | dm.xpt           | DM           | Demographics
       1          | USUBJID       | 001            | dm.xpt           | DM           | Demographics
       2          | STUDYID       | ABC123         | dm.xpt           | DM           | Demographics
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_values_with_dataset_metadata"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )

        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        if not source_schema:
            raise ValueError(f"Source table {source_table_id} not found")

        columns_list = source_schema.get_columns()
        column_names = [name for name, _ in columns_list if name != "id"]  # skip the id column

        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)
        count_query = f"SELECT COUNT(*) as count FROM {source_table_hash};"
        self.data_service.pgi.execute_sql(count_query)
        count_result = self.data_service.pgi.fetch_all()
        record_count = count_result[0]["count"] if count_result else 0

        dataset_location = self.dataset_metadata.filename
        dataset_name = self.dataset_metadata.name
        dataset_label = self.dataset_metadata.label or ""

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("row_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_value", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_location", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("record_count", "Num"))

        self.data_service.pgi.create_table(schema)

        # build UNPIVOT SQL query (postgresql doesn't have UNPIVOT so we use UNION ALL)
        if column_names:
            select_statements = []
            for col_name in column_names:
                col_hash = source_schema.get_column_hash(col_name)
                select_statements.append(
                    f"""
                    SELECT
                        ROW_NUMBER() OVER () as row_number,
                        '{col_name.upper()}' as variable_name,
                        CAST({col_hash} AS TEXT) as variable_value,
                        '{dataset_location}' as dataset_location,
                        '{dataset_name}' as dataset_name,
                        '{dataset_label}' as dataset_label,
                        {record_count} as record_count
                    FROM {source_table_hash}
                """
                )

            unpivot_query = " UNION ALL ".join(select_statements)

            insert_query = f"""
                INSERT INTO {schema.hash}
                ({schema.get_column_hash("row_number")},
                 {schema.get_column_hash("variable_name")},
                 {schema.get_column_hash("variable_value")},
                 {schema.get_column_hash("dataset_location")},
                 {schema.get_column_hash("dataset_name")},
                 {schema.get_column_hash("dataset_label")},
                 {schema.get_column_hash("record_count")})
                {unpivot_query};
            """

            self.data_service.pgi.execute_sql(insert_query)

        return table_name
