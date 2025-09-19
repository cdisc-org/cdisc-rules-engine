import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Union

from cdisc_rules_engine.data_service.loading.load_datasets import SqlDatasetLoader
from cdisc_rules_engine.data_service.loading.load_test_datasets import (
    SqlTestDatasetLoader,
)
from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.startup.populate_codelists import (
    populate_codelists,
)
from cdisc_rules_engine.data_service.startup.populate_standards import (
    populate_standards,
)
from cdisc_rules_engine.data_service.startup.populate_terminology import (
    populate_terminology,
)
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.utilities.ig_specification import IGSpecification

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).parent / "schemas"


@dataclass
class SQLDatasetMetadata:
    filename: str
    filepath: str
    dataset_id: str
    table_hash: str
    dataset_name: str
    dataset_label: str
    unsplit_name: str
    domain: str
    is_supp: bool
    rdomain: str
    variables: list[str]
    is_split: bool = False


class PostgresQLDataService:

    def __init__(self, postgres_interface: PostgresQLInterface, standard: IGSpecification):
        self.pgi = postgres_interface
        self.datasets: List[DatasetMetadata] = []
        self.ig_specs = standard

    @classmethod
    def instance(cls, standard: IGSpecification = None) -> "PostgresQLDataService":
        """
        Create a PostgresQLDataService instance with an initialized database.
        """
        # PostgresDB setup
        pgi = PostgresQLInterface()
        pgi.init_database()

        instance = cls(postgres_interface=pgi, standard=standard)
        pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))
        populate_terminology(pgi)
        populate_codelists(pgi)
        populate_standards(pgi)
        return instance

    @classmethod
    def from_list_of_testdatasets(
        cls, test_datasets: list[TestDataset], standard: IGSpecification = None
    ) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables
        """
        instance = cls.instance(standard)
        instance.datasets += SqlTestDatasetLoader.load_test_datasets(instance.pgi, test_datasets)
        return instance

    @classmethod
    def from_dataset_paths(cls, datasets_path: Path, standard: IGSpecification = None) -> "PostgresQLDataService":
        instance = cls.instance(standard)
        instance.datasets += SqlDatasetLoader.load_datasets(instance.pgi, datasets_path)
        return instance

    @staticmethod
    def add_test_dataset(
        data_service: "PostgresQLDataService", table_name: str, column_data: dict[str, list[Union[str, int, float]]]
    ):
        # Check all the columns are the same length
        lengths = {len(v) for v in column_data.values()}
        if len(set(lengths)) != 1:
            raise ValueError("All input data columns must have the same length")

        # Create schema and table:
        schema_row = {
            col.lower(): next((val for val in values if val is not None), "") for col, values in column_data.items()
        }
        row_dicts = [dict(zip(column_data, values)) for values in zip(*column_data.values())]
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        schema = SqlTableSchema.from_data(table_name, schema_row)
        data_service.pgi.create_table(schema)

        data_service.pgi.insert_data(table_name=table_name, data=row_dicts)

        data_service.datasets.append(
            SDTMDatasetMetadata(
                file_size=0,
                filename=f"{table_name}.xpt",
                full_path=f"/test/{table_name}.xpt",
                label=f"Test {table_name} Dataset",
                name=table_name,
                record_count=len(row_dicts),
                modification_date=None,
                original_path=None,
                first_record=row_dicts[0],
            )
        )

        return schema

    def get_uploaded_dataset_ids(self) -> list[str]:
        return [dataset.name for dataset in self.datasets]

    def get_dataset_metadata(self, dataset_id: str) -> SQLDatasetMetadata:

        tmp = next((metadata for metadata in self.datasets if metadata.name.lower() == dataset_id.lower()), None)
        if not tmp:
            return None
        return SQLDatasetMetadata(
            filename=tmp.filename,
            filepath=str(tmp.full_path),
            dataset_id=tmp.name,
            table_hash=tmp.name,
            dataset_name=tmp.name,
            dataset_label=tmp.label,
            unsplit_name=tmp.name,
            domain=tmp.domain,
            is_supp=tmp.is_supp,
            rdomain=tmp.rdomain,
            variables=[],
            is_split=tmp.is_split,
        )

    def get_dataset_for_rule(self, dataset_metadata: SQLDatasetMetadata, rule: dict) -> str:
        """Get or create preprocessed dataset based on rule requirements."""
        datasets = rule.get("datasets", [])
        if not datasets:
            return dataset_metadata.dataset_id

        left_id = dataset_metadata.dataset_id

        for merge_spec in datasets:
            right = merge_spec.get("domain_name").lower()

            # TODO: This only handles simple joins for now
            if right in ("relrec", "supp--", "relsub", "co", "sq"):
                raise NotImplementedError("Joins with relationship domains are not supported yet")

            join_type = merge_spec.get("join_type", "INNER")
            # For now we assume pivot columns are always the same in left and right
            pivot_columns = merge_spec.get("match_key", [])

            joined_schema = SqlJoinMerge.perform_join(
                pgi=self.pgi,
                left=self.pgi.schema.get_table(left_id),
                right=self.pgi.schema.get_table(right),
                pivot_left=pivot_columns,
                pivot_right=pivot_columns,
                type=join_type.upper(),
            )
            left_id = joined_schema.name

        return left_id
