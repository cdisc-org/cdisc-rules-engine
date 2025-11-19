from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlDatasetMetadataBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Dataset Metadata Check rules.
    Creates a table with a single row containing dataset metadata.

    Example table structure:
    dataset_location | dataset_name | dataset_label | record_count
    -----------------|--------------|---------------|-------------
    dm.xpt           | DM           | Demographics  | 100
    """

    def build(self) -> str:
        """
        Create dataset metadata table and return table name.
        """
        table_name = f"{self.dataset_metadata.name}_dataset_metadata"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        schema = SqlTableSchema.static(table_name)
        schema.add_column(SqlColumnSchema.generated("dataset_location", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("record_count", "Num"))

        self.data_service.pgi.create_table(schema)

        count_query = f"SELECT COUNT(*) as count FROM {self.dataset_metadata.name};"
        self.data_service.pgi.execute_sql(count_query)
        count_result = self.data_service.pgi.fetch_all()
        record_count = count_result[0]["count"] if count_result else 0

        row = {
            "dataset_location": self.dataset_metadata.filename,
            "dataset_name": self.dataset_metadata.name,
            "dataset_label": self.dataset_metadata.label or "",
            "record_count": record_count,
        }

        self.data_service.pgi.insert_data(table_name, [row])
        return table_name
