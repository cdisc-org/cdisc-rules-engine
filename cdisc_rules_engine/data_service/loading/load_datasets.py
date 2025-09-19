import logging
from typing import List

from zipp import Path

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.readers.data_reader import DataReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SqlDatasetLoader:

    @staticmethod
    def load_datasets(pgi: PostgresQLInterface, directory_path: Path) -> List[DatasetMetadata]:
        """
        Iterate through dataset files in `self.datasets_path`
        and create corresponding SQL tables.
        """
        return [SqlDatasetLoader._load_dataset_file(pgi, file_path) for file_path in directory_path.iterdir()]

    @staticmethod
    def _load_dataset_file(pgi: PostgresQLInterface, file_path: Path) -> DatasetMetadata:
        """Load a single dataset file."""
        try:
            reader = DataReader(str(file_path))
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
                chunk_data = [{k.lower(): v for k, v in row} for row in chunk_data.items()]
                if not first_record:
                    first_record = chunk_data[0] if chunk_data else None
                pgi.insert_data(table_name, chunk_data)

            logger.info(f"Successfully loaded {file_path.name}")

            return SDTMDatasetMetadata(
                file_size=0,
                filename=metadata_info["name"],
                full_path=file_path,
                label=metadata_info["label"],
                name=metadata_info["name"],
                record_count=metadata_info["record_count"],
                modification_date=None,
                original_path=None,
                first_record={k.upper(): v for k, v in first_record.items()},
            )
        except Exception as e:
            logger.error(f"Failed to load {file_path.name}: {e}")
