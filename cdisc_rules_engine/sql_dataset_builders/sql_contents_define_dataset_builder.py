from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_DATASETS_TYPE,
)


class SqlContentsDefineDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Dataset Contents Check against Define XML rules.
    Adds dataset metadata from Define XML to the dataset for use in rules.
    """

    def build(self) -> str:
        table_id = self.data_service.get_dataset_for_rule(self.dataset_metadata, self.rule, self.standards_context)

        schema = self.data_service.pgi.schema.get_table(table_id)
        if not schema:
            raise ValueError(f"Table {table_id} not found")

        for col in ["dataset_location", "dataset_name", "dataset_label", "dataset_domain"]:
            self.data_service.pgi.add_column(table_id, SqlColumnSchema.define(col, "Char"))

        define_ds_metadata = self.get_define_dataset()
        for col, type in DEFINE_DATASETS_TYPE.items():
            self.data_service.pgi.add_column(table_id, SqlColumnSchema.define(col, type))

        dataset_location = self.dataset_metadata.filename
        dataset_name = self.dataset_metadata.name
        dataset_label = self.dataset_metadata.label
        dataset_domain = self.dataset_metadata.domain

        table_hash = self.data_service.pgi.schema.get_table_hash(table_id)

        row = {
            "dataset_location": dataset_location,
            "dataset_name": dataset_name,
            "dataset_label": dataset_label,
            "dataset_domain": dataset_domain,
        }
        row.update(define_ds_metadata)

        set_values = []
        for col, value in row.items():
            col_hash = self.data_service.pgi.schema.get_column_hash(table_id, col)
            if value is None:
                set_values.append(f"{col_hash} = NULL")
            elif isinstance(value, str):
                set_values.append(f"{col_hash} = '{value}'")
            else:
                set_values.append(f"{col_hash} = {value}")
        set_query = ", ".join(set_values)

        update_query = f"UPDATE {table_hash} SET {set_query};"
        self.data_service.pgi.execute_sql(update_query)

        self.data_service.pgi.add_column(table_id, SqlColumnSchema.define("define_key_sequence_is_unique", "Bool"))

        unique_col_hash = self.data_service.pgi.schema.get_column_hash(table_id, "define_key_sequence_is_unique")
        uniqueness_query = f"""
            UPDATE {table_hash} t
            SET {unique_col_hash} = sub.unique_status
            FROM (
                SELECT id,
                CASE
                    WHEN COUNT(*) OVER (PARTITION BY {define_ds_metadata['define_dataset_key_sequence']}) = 1 THEN TRUE
                    ELSE FALSE
                END as unique_status
                FROM {table_hash}
            ) sub
            WHERE t.id = sub.id;
        """
        self.data_service.pgi.execute_sql(uniqueness_query)

        return table_id
