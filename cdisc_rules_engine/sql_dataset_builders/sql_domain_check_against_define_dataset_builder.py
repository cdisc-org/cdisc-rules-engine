from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import SqlBaseDatasetBuilder, DEFINE_DATASETS_TYPE


class SqlDomainCheckAgainstDefineDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Creates a table merging the physical dataset metadata with Define-XML dataset metadata.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_ds_metadata_with_define"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        domain_files = {ds.domain: ds.filename for ds in self.datasets}
        define_ds_metadata = self.get_define_metadata()

        rows = []

        for define_metadata in define_ds_metadata:
            define_dataset_name = define_metadata.get("define_dataset_name")
            row = {
                "domain": define_dataset_name,
                "filename": domain_files.get(define_dataset_name, None),
            }
            define_metadata_dict = {k: define_metadata.get(k) for k in DEFINE_DATASETS_TYPE.keys()}
            row.update(define_metadata_dict)
            rows.append(row)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        schema.add_column(SqlColumnSchema.generated("domain", "Char"))
        schema.add_column(SqlColumnSchema.generated("filename", "Char"))
        for col, type in DEFINE_DATASETS_TYPE.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        self.data_service.pgi.create_table(schema)
        if rows:
            self.data_service.pgi.insert_data(table_name, rows)

        return table_name
