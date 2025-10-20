from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, List, Union

from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER
from cdisc_rules_engine.data_service.loading.load_datasets import SqlDatasetLoader
from cdisc_rules_engine.data_service.loading.load_test_datasets import (
    SqlTestDatasetLoader,
)
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
from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)
from cdisc_rules_engine.models.sql.table_schema import SqlColumnSchema, SqlTableSchema
from cdisc_rules_engine.models.test_dataset import TestDataset

if TYPE_CHECKING:  # Only imports the below statements during type checking
    from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext


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
    variables: list[VariableMetadata]
    is_split: bool = False


class PostgresQLDataService:

    def __init__(self, postgres_interface: PostgresQLInterface):
        self.pgi = postgres_interface
        self.datasets: List[DatasetMetadata2] = []

    @classmethod
    def instance(cls) -> "PostgresQLDataService":
        """
        Create a PostgresQLDataService instance with an initialized database.
        """
        # PostgresDB setup
        pgi = PostgresQLInterface()
        pgi.init_database()

        instance = cls(postgres_interface=pgi)
        pgi.execute_sql_file(str(SCHEMA_PATH / "clinical_data_metadata_schema.sql"))
        populate_terminology(pgi)
        populate_codelists(pgi)
        populate_standards(pgi)
        return instance

    @classmethod
    def from_list_of_testdatasets(cls, test_datasets: list[TestDataset]) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables
        """
        instance = cls.instance()
        instance.datasets += SqlTestDatasetLoader.load_test_datasets(instance.pgi, test_datasets)
        return instance

    @classmethod
    def from_dataset_paths(cls, dataset_paths: List[str]) -> "PostgresQLDataService":
        instance = cls.instance()
        instance.datasets += SqlDatasetLoader.load_datasets(instance.pgi, dataset_paths)
        return instance

    @staticmethod
    def add_test_dataset(
        data_service: "PostgresQLDataService", table_name: str, column_data: dict[str, list[Union[str, int, float]]]
    ):
        # Check all the columns are the same length
        lengths = {len(v) for v in column_data.values()}
        if len(set(lengths)) != 1:
            raise ValueError("All input data columns must have the same length")

        if SOURCE_ROW_NUMBER in [k.lower() for k in column_data.keys()]:
            raise ValueError(
                f"Test dataset '{table_name}' contains reserved column 'source_row_number'. "
                "This column is automatically generated and should not be in test data."
            )

        # Create schema and table:
        schema_row = {
            col.lower(): next((val for val in values if val is not None), "") for col, values in column_data.items()
        }
        row_dicts = [dict(zip(column_data, values)) for values in zip(*column_data.values())]
        row_dicts = [{k.lower(): v for k, v in row.items()} for row in row_dicts]

        for idx, row in enumerate(row_dicts, start=1):
            row[SOURCE_ROW_NUMBER] = idx

        schema = SqlTableSchema.from_data(table_name, schema_row)
        source_row_column = SqlColumnSchema(name=SOURCE_ROW_NUMBER, hash=SOURCE_ROW_NUMBER, type="Num")
        schema.add_column(source_row_column)
        data_service.pgi.create_table(schema)

        data_service.pgi.insert_data(table_name=table_name, data=row_dicts)

        data_service.datasets.append(
            DatasetMetadata2(
                filename=f"{table_name}.xpt",
                name=table_name,
                label=f"Test {table_name} Dataset",
                variables=[
                    VariableMetadata(
                        name=col,
                        order=i + 1,
                        label=f"Test {col} Variable",
                        length=200,
                        type="Char" if isinstance(next((val for val in values if val is not None), ""), str) else "Num",
                        format="",
                    )
                    for i, (col, values) in enumerate(column_data.items())
                ],
            )
        )

        return schema

    def get_uploaded_dataset_ids(self) -> list[str]:
        return [dataset.name for dataset in self.datasets]

    def get_dataset_metadata(self, dataset_id: str, standards_context: "BaseStandardsContext") -> SQLDatasetMetadata:
        tmp = next((metadata for metadata in self.datasets if metadata.name.lower() == dataset_id.lower()), None)
        if not tmp:
            return None
        domain = standards_context.derive_domain(tmp.name)
        return SQLDatasetMetadata(
            filename=tmp.filename,
            filepath=tmp.filename,
            dataset_id=tmp.name,
            table_hash=tmp.name,
            dataset_name=tmp.name,
            dataset_label=tmp.label,
            unsplit_name=tmp.name,
            domain=domain,
            # Clearly not going to stay here
            is_supp=domain == "SUPPQUAL",
            rdomain=self.name[4:].upper() if domain.startswith("supp") else None,
            variables=tmp.variables,
            is_split=tmp.name.startswith(domain.lower()) and tmp.name != domain.lower(),
        )

    def get_dataset_for_rule(
        self, dataset_metadata: SQLDatasetMetadata, rule: dict, standards_context: "BaseStandardsContext"
    ) -> str:
        """Get or create preprocessed dataset based on rule requirements."""
        datasets = rule.get("datasets", [])
        if not datasets:
            return dataset_metadata.dataset_id

        left_id = dataset_metadata.dataset_id

        for merge_spec in datasets:
            left_id = standards_context.perform_merge(
                data_service=self,
                original=left_id,
                dataset_metadata=dataset_metadata,
                merge_spec=merge_spec,
                rule=rule,
            )

        return left_id

    # Temporarily adding this method to get the report to output
    def read_data(self, path: str):
        return None
