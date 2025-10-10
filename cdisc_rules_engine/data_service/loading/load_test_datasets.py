from datetime import datetime
from typing import List

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema, SqlColumnSchema
from cdisc_rules_engine.models.test_dataset import TestDataset


class SqlTestDatasetLoader:
    @staticmethod
    def load_test_datasets(pgi: PostgresQLInterface, test_datasets: List[TestDataset]) -> List[DatasetMetadata]:
        return [SqlTestDatasetLoader.load_test_dataset(pgi, test_dataset) for test_dataset in test_datasets]

    @staticmethod
    def _map_test_type_to_sql_type(test_type: str) -> str:
        """Map test dataset type to SQL type (text or numeric)."""
        if not test_type:
            return "text"

        type_str = str(test_type).strip()

        # Map DATASET_COLUMN_TYPES to SQL types
        if type_str == "Num":
            return "numeric"
        elif type_str == "Char":
            return "text"
        else:
            # Default to text for unknown types
            return "text"

    @staticmethod
    def _populate_data_metadata(
        pgi: PostgresQLInterface,
        test_dataset: TestDataset,
        table_name: str,
        first_record: dict,
    ) -> None:
        """Populate data_metadata table with variable information from test dataset."""
        timestamp = datetime.now().astimezone()

        # Ensure data_metadata table exists in schema registry
        if pgi.schema.get_table("data_metadata") is None:
            # Register data_metadata table in schema
            metadata_schema = SqlTableSchema.static("data_metadata")
            metadata_schema.add_column(SqlColumnSchema.generated("id", "Num"))
            metadata_schema.add_column(SqlColumnSchema.generated("created_at", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("updated_at", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_filename", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_filepath", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_id", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("table_hash", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_name", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_label", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_domain", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_is_supp", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_rdomain", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_is_split", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_unsplit_name", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("dataset_preprocessed", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("var_name", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("var_label", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("var_type", "Char"))
            metadata_schema.add_column(SqlColumnSchema.generated("var_length", "Num"))
            metadata_schema.add_column(SqlColumnSchema.generated("var_format", "Char"))
            pgi.schema.add_table(metadata_schema)

        # Get domain from first_record if available
        domain = first_record.get("domain") if first_record else None

        # Insert one row per variable into data_metadata
        for var_metadata in test_dataset.get("variables", []):
            var_name = var_metadata.get("name", "").lower()
            var_label = var_metadata.get("label", "")
            var_type = SqlTestDatasetLoader._map_test_type_to_sql_type(var_metadata.get("type", ""))
            var_length = var_metadata.get("length", 0)
            var_format = var_metadata.get("format", "")

            # Escape single quotes for SQL
            var_label_escaped = var_label.replace("'", "''") if var_label else ""
            var_format_escaped = var_format.replace("'", "''") if var_format else ""
            dataset_label_escaped = test_dataset.get("label", "").replace("'", "''")

            # Handle NULL values for SQL insert
            label_sql = f"'{var_label_escaped}'" if var_label else "NULL"
            format_sql = f"'{var_format_escaped}'" if var_format else "NULL"
            domain_sql = f"'{domain}'" if domain else "NULL"

            # Build SQL insert statement manually since data_metadata table doesn't use hashed column names
            insert_sql = f"""
                INSERT INTO data_metadata (
                    created_at, updated_at,
                    dataset_filename, dataset_filepath, dataset_id, table_hash,
                    dataset_name, dataset_label, dataset_domain,
                    dataset_is_supp, dataset_rdomain,
                    dataset_is_split, dataset_unsplit_name, dataset_preprocessed,
                    var_name, var_label, var_type, var_length, var_format
                ) VALUES (
                    '{timestamp.isoformat()}', '{timestamp.isoformat()}',
                    '{test_dataset["filename"]}', '{test_dataset["filepath"]}', '{table_name}', '{table_name}',
                    '{test_dataset["name"]}', '{dataset_label_escaped}', {domain_sql},
                    FALSE, NULL,
                    FALSE, '{test_dataset["name"]}', NULL,
                    '{var_name}', {label_sql}, '{var_type}', {var_length}, {format_sql}
                );
            """
            pgi.execute_sql(insert_sql)

    @staticmethod
    def load_test_dataset(pgi: PostgresQLInterface, test_dataset: TestDataset) -> DatasetMetadata:
        # Create schema and table:
        row_dicts = [dict(zip(test_dataset["records"], values)) for values in zip(*test_dataset["records"].values())]
        # force lower_case throughout
        table_name = test_dataset["name"].lower()
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        if row_dicts and SOURCE_ROW_NUMBER in row_dicts[0]:
            raise ValueError(
                f"Test dataset '{table_name}' contains reserved column 'source_row_number'. "
                "This column is automatically generated and should not be in test data."
            )

        for idx, row in enumerate(row_dicts, start=1):
            row[SOURCE_ROW_NUMBER] = idx

        schema = SqlTableSchema.from_metadata(test_dataset)
        source_row_column = SqlColumnSchema(name=SOURCE_ROW_NUMBER, hash=SOURCE_ROW_NUMBER, type="Num")
        schema.add_column(source_row_column)

        pgi.create_table(schema)
        pgi.insert_data(table_name=table_name, data=row_dicts)

        # TODO INDEX

        # Populate data_metadata table with variable information
        first_record = {k.lower(): v for k, v in row_dicts[0].items()} if row_dicts else {}
        SqlTestDatasetLoader._populate_data_metadata(pgi, test_dataset, table_name, first_record)

        return SDTMDatasetMetadata(
            file_size=0,
            filename=test_dataset["filename"],
            full_path=test_dataset["filepath"],
            label=test_dataset["label"],
            name=test_dataset["name"],
            record_count=len(row_dicts),
            modification_date=None,
            original_path=None,
            first_record={k.upper(): v for k, v in row_dicts[0].items()},
        )
