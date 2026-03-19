from __future__ import annotations

from dataclasses import dataclass
from io import IOBase
from typing import TYPE_CHECKING, Dict, List, Union, Optional

from cdisc_rules_engine.data_service.loading.load_datasets import SqlDatasetLoader
from cdisc_rules_engine.data_service.loading.load_test_datasets import (
    SqlTestDatasetLoader,
)
from cdisc_rules_engine.models.sql_external_dictionaries_container import SqlExternalDictionariesContainer
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.sql_data_preprocessor import SqlDataPreprocessor
from cdisc_rules_engine.data_service.startup.populate_codelists import (
    populate_codelists,
)
from cdisc_rules_engine.data_service.startup.populate_standards import (
    populate_standards,
)
from cdisc_rules_engine.data_service.startup.populate_dictionaries import (
    populate_dictionaries,
)
from cdisc_rules_engine.models.dataset_metadata2 import (
    VariableMetadata,
)
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.data_service.database import (
    DatabaseConfigPostgres,
    DatabaseConfigPGServer,
)

if TYPE_CHECKING:  # Only imports the below statements during type checking
    from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext


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
        self.datasets: List[BaseDatasetMetadata] = []

    @classmethod
    def instance(
        cls,
        sql_namespace: Optional[str] = None,
        use_pgserver: bool = False,
        codelists: Optional[List[Union[str, Dict]]] = None,
        external_dictionaries: Optional[SqlExternalDictionariesContainer] = None,
        cache_path: Optional[str] = None,
        define_xml_path: Optional[str] = None,
    ) -> "PostgresQLDataService":
        """
        Create a PostgresQLDataService instance with an initialized database.
        """
        # PostgresDB setup
        pgi = PostgresQLInterface(
            sql_namespace=sql_namespace,
            config=(DatabaseConfigPGServer() if use_pgserver else DatabaseConfigPostgres()),
        )
        pgi.init_database()

        instance = cls(postgres_interface=pgi)
        populate_dictionaries(pgi, external_dictionaries)
        populate_codelists(pgi, cache_path, codelists)
        populate_standards(pgi)

        instance._update_define_xml_path(define_xml_path)

        return instance

    @classmethod
    def from_list_of_testdatasets(
        cls,
        test_datasets: list[TestDataset],
        standards_context: BaseStandardsContext,
        use_pgserver: bool = False,
        cache_path: Optional[str] = None,
        define_xml_path: Optional[str] = None,
    ) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables
        """
        instance = cls.instance(use_pgserver=use_pgserver, cache_path=cache_path, define_xml_path=define_xml_path)
        instance.datasets += [
            standards_context.transform_dataset_metadata(SqlTestDatasetLoader.load_test_dataset(instance.pgi, ds))
            for ds in test_datasets
        ]
        SqlDataPreprocessor.run(instance, standards_context)
        return instance

    @classmethod
    def from_dataset_paths(
        cls,
        dataset_paths,
        standards_context,
        codelists: Optional[List[Union[str, Dict]]] = None,
        external_dictionaries: Optional[SqlExternalDictionariesContainer] = None,
        cache_path: Optional[str] = None,
        define_xml_path: Optional[str] = None,
        sql_namespace: Optional[str] = None,
        use_pgserver: bool = False,
    ) -> "PostgresQLDataService":
        instance = cls.instance(
            sql_namespace=sql_namespace,
            use_pgserver=use_pgserver,
            codelists=codelists,
            cache_path=cache_path,
            external_dictionaries=external_dictionaries,
            define_xml_path=define_xml_path,
        )

        instance.datasets.extend(
            standards_context.transform_dataset_metadata(ds)
            for ds in SqlDatasetLoader.load_datasets(instance.pgi, dataset_paths)
        )
        SqlDataPreprocessor.run(instance, standards_context)
        return instance

    @staticmethod
    def add_test_dataset(
        data_service: "PostgresQLDataService",
        table_name: str,
        column_data: dict[str, list[Union[str, int, float]]],
        standards_context: BaseStandardsContext,
    ):
        dataset = TestDataset.from_records(table_name, column_data)
        metadata = standards_context.transform_dataset_metadata(
            SqlTestDatasetLoader.load_test_dataset(data_service.pgi, dataset)
        )
        data_service.datasets.append(metadata)
        return data_service.pgi.schema.get_table(metadata.name)

    def get_uploaded_dataset_ids(self) -> list[str]:
        return [dataset.name for dataset in self.datasets]

    def get_dataset_metadata(self, dataset_id: str) -> BaseDatasetMetadata:
        return next((metadata for metadata in self.datasets if metadata.name.lower() == dataset_id.lower()), None)

    def get_dataset_for_rule(
        self, dataset_metadata: BaseDatasetMetadata, rule: dict, standards_context: "BaseStandardsContext"
    ) -> str:
        """Get or create preprocessed dataset based on rule requirements."""
        datasets = rule.get("datasets", [])
        if not datasets:
            return dataset_metadata.name

        left_id = dataset_metadata.name

        for merge_spec in datasets:
            left_id = standards_context.perform_merge(
                data_service=self,
                original=left_id,
                dataset_metadata=dataset_metadata,
                merge_spec=merge_spec,
                rule=rule,
            )

        return left_id

    def read_data(self, file_path: str) -> IOBase:
        return open(file_path, "rb")

    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        """
        Reads local define xml file as bytes
        """
        with open(dataset_name, "rb") as f:
            return f.read()

    def _update_define_xml_path(self, define_xml_path: str):
        self.define_xml_path = define_xml_path
