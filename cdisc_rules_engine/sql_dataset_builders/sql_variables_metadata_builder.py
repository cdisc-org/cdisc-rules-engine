from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlVariablesMetadataBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Variable Metadata Check rules.
    Creates a table with variable metadata for the specific dataset.
    Queries data_metadata table to get variable metadata (labels, formats, types).

    Example table structure (one row per variable):
       variable_name | variable_order_number | variable_label | variable_size | variable_data_type | variable_format
    -----------------|----------------------|----------------|---------------|-------------------|----------------
       STUDYID       | 1                    | Study ID       | 16            | text              | $16.
       USUBJID       | 2                    | Subject ID     | 20            | text              | $20.
       AETERM        | 3                    | AE Term        | 200           | text              | $200.
    """

    def build(self) -> str:
        """
        Create variable metadata table for this dataset and return table name.
        """
        table_name = f"{self.dataset_metadata.dataset_id}_var_metadata"

        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        query = f"""
            SELECT
                var_name,
                var_label,
                var_type,
                var_length,
                var_format
            FROM data_metadata
            WHERE LOWER(dataset_id) = LOWER('{self.dataset_metadata.dataset_id}')
            ORDER BY id
        """

        self.data_service.pgi.execute_sql(query)
        variables_info = self.data_service.pgi.fetch_all()

        if not variables_info:
            raise ValueError(f"No variables found in data_metadata for dataset: {self.dataset_metadata.dataset_id}")

        # Create table schema with required columns
        schema = SqlTableSchema.static(table_name)
        schema.add_column(SqlColumnSchema.generated("variable_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_order_number", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_size", "Num"))
        schema.add_column(SqlColumnSchema.generated("variable_data_type", "Char"))
        schema.add_column(SqlColumnSchema.generated("variable_format", "Char"))

        self.data_service.pgi.create_table(schema)

        for idx, var_info in enumerate(variables_info, start=1):
            column_hashes = {
                "variable_name": schema.get_column_hash("variable_name"),
                "variable_order_number": schema.get_column_hash("variable_order_number"),
                "variable_label": schema.get_column_hash("variable_label"),
                "variable_size": schema.get_column_hash("variable_size"),
                "variable_data_type": schema.get_column_hash("variable_data_type"),
                "variable_format": schema.get_column_hash("variable_format"),
            }

            # Map data_metadata fields to variable metadata columns
            variable_name = var_info["var_name"].upper()
            variable_order_number = idx
            variable_label = var_info["var_label"]
            variable_size = var_info["var_length"] or 0
            variable_data_type = var_info["var_type"]
            variable_format = var_info["var_format"]

            label_sql = f"'{variable_label}'" if variable_label else "NULL"
            format_sql = f"'{variable_format}'" if variable_format else "NULL"

            insert_sql = f"""
                INSERT INTO {schema.hash} (
                    {column_hashes['variable_name']},
                    {column_hashes['variable_order_number']},
                    {column_hashes['variable_label']},
                    {column_hashes['variable_size']},
                    {column_hashes['variable_data_type']},
                    {column_hashes['variable_format']}
                )
                VALUES (
                    '{variable_name}',
                    {variable_order_number},
                    {label_sql},
                    {variable_size},
                    '{variable_data_type}',
                    {format_sql}
                );
            """

            self.data_service.pgi.execute_sql(insert_sql)

        return table_name
