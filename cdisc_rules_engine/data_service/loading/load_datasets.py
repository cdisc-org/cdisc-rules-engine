from pathlib import Path
from typing import List

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
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
            pgi.create_table(schema)
            # TODO: INDEX

            first_record = None

            for chunk_data in reader.read():
                # force lowercase on columns
                chunk_data = [{k.lower(): v for k, v in row.items()} for row in chunk_data]
                if not first_record:
                    first_record = chunk_data[0] if chunk_data else None
                pgi.insert_data(table_name, chunk_data)

            logger.info(f"Successfully loaded {file_path.name}")

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
