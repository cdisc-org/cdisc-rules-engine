from datetime import datetime
from pathlib import Path
from typing import List

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema, SqlColumnSchema
from cdisc_rules_engine.readers.data_reader import DataReader
from cdisc_rules_engine.services import logger


class SqlDatasetLoader:

    @staticmethod
    def load_datasets(pgi: PostgresQLInterface, dataset_paths: List[str]) -> List[DatasetMetadata]:
        """
        Iterate through dataset files in `self.dataset_paths`
        and create corresponding SQL tables.
        """
        return [SqlDatasetLoader._load_dataset_file(pgi, file_path) for file_path in dataset_paths]

    @staticmethod
    def _map_sas_type_to_sql_type(sas_type: str) -> str:
        """Map SAS type to SQL type (text or numeric)."""
        if not sas_type:
            return "text"

        type_lower = str(sas_type).lower().strip()

        # Numeric types
        if type_lower in ["d", "double", "n", "num", "numeric", "i", "int", "integer", "f", "float"]:
            return "numeric"
        # Text types
        elif type_lower in ["s", "string", "c", "char", "character", "text"]:
            return "text"
        else:
            # Default to text for unknown types
            return "text"

    @staticmethod
    def _populate_data_metadata(
        pgi: PostgresQLInterface,
        file_path_str: str,
        table_name: str,
        metadata_info: dict,
        first_record: dict,
    ) -> None:
        """Populate data_metadata table with variable information from XPT metadata."""
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
        for var_metadata in metadata_info.get("variables", []):
            var_name = var_metadata.get("name", "").lower()

            # Handle bytes values from XPT files
            var_label = var_metadata.get("label", "")
            if isinstance(var_label, bytes):
                var_label = var_label.decode("utf-8", errors="replace").strip()

            var_type = SqlDatasetLoader._map_sas_type_to_sql_type(var_metadata.get("type", ""))
            var_length = var_metadata.get("length", 0)

            var_format = var_metadata.get("format", "")
            if isinstance(var_format, bytes):
                var_format = var_format.decode("utf-8", errors="replace").strip()

            # Escape single quotes for SQL
            var_label_escaped = var_label.replace("'", "''") if var_label else ""
            var_format_escaped = var_format.replace("'", "''") if var_format else ""
            dataset_label_escaped = metadata_info.get("label", "").replace("'", "''")

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
                    '{metadata_info["name"]}.xpt', '{file_path_str}', '{table_name}', '{table_name}',
                    '{metadata_info["name"]}', '{dataset_label_escaped}', {domain_sql},
                    FALSE, NULL,
                    FALSE, '{metadata_info["name"]}', NULL,
                    '{var_name}', {label_sql}, '{var_type}', {var_length}, {format_sql}
                );
            """
            pgi.execute_sql(insert_sql)

    @staticmethod
    def _load_dataset_file(pgi: PostgresQLInterface, file_path_str: str) -> DatasetMetadata:
        """Load a single dataset file."""
        file_path = Path(file_path_str)
        try:
            reader = DataReader(file_path_str)
            metadata_info = reader.read_metadata()

            # force table_name to be lowercase
            table_name = file_path.stem.lower()

            logger.info(f"Loading dataset {file_path.name} into table {table_name}")

            schema = SqlTableSchema.from_metadata(metadata_info)
            source_row_column = SqlColumnSchema(name=SOURCE_ROW_NUMBER, hash=SOURCE_ROW_NUMBER, type="Num")
            schema.add_column(source_row_column)

            pgi.create_table(schema)
            # TODO: INDEX

            first_record = None
            row_number = 0

            for chunk_data in reader.read():
                # force lowercase on columns
                chunk_data = [{k.lower(): v for k, v in row.items()} for row in chunk_data]

                if chunk_data and SOURCE_ROW_NUMBER in chunk_data[0]:
                    raise ValueError(
                        f"Dataset file '{file_path.name}' contains reserved column 'source_row_number'. "
                        "This column is automatically generated and should not be in source data."
                    )

                for row in chunk_data:
                    row_number += 1
                    row[SOURCE_ROW_NUMBER] = row_number

                if not first_record:
                    first_record = chunk_data[0] if chunk_data else None
                pgi.insert_data(table_name, chunk_data)

            logger.info(f"Successfully loaded {file_path.name}")

            # Populate data_metadata table with variable information
            SqlDatasetLoader._populate_data_metadata(pgi, file_path_str, table_name, metadata_info, first_record)

            return SDTMDatasetMetadata(
                file_size=0,
                filename=metadata_info["name"],
                full_path=file_path_str,
                label=metadata_info["label"],
                name=metadata_info["name"],
                record_count=metadata_info["record_count"],
                modification_date=None,
                original_path=None,
                first_record={k.upper(): v for k, v in first_record.items()},
            )
        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")
