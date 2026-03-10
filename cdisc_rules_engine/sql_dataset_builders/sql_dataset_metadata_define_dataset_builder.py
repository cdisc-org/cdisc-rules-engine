from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder, DEFINE_DATASETS_TYPE


class SqlDatasetMetadataWithDefineDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Creates a table merging the physical dataset metadata with Define-XML dataset metadata.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_ds_metadata_with_define"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        all_ds_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]
        define_ds_metadata = self.get_define_all_datasets()

        rows = []

        for ds_metadata in all_ds_metadata:
            row = {
                "dataset_name": ds_metadata.name,
                "dataset_location": ds_metadata.filename,
                "dataset_label": ds_metadata.label or "",
                "dataset_domain": ds_metadata.domain,
            }
            define_metadata = define_ds_metadata.get(ds_metadata.domain, {})
            row.update(define_metadata)
            rows.append(row)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_location", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_label", "Char"))
        schema.add_column(SqlColumnSchema.generated("dataset_domain", "Char"))
        for col, type in DEFINE_DATASETS_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)
        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name
