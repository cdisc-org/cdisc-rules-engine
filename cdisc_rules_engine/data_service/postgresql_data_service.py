from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from io import IOBase
from typing import TYPE_CHECKING, Any, Dict, List, Union, Optional

from cdisc_rules_engine.data_service.loading.load_datasets import SqlDatasetLoader
from cdisc_rules_engine.data_service.loading.load_test_datasets import (
    SqlTestDatasetLoader,
)
from cdisc_rules_engine.models.sql_external_dictionaries_container import SqlExternalDictionariesContainer
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.sql_data_preprocessor import SqlDataPreprocessor
from cdisc_rules_engine.data_service.startup.populate_codelists import (
    populate_codelists,
    populate_latest_codelist,
    populate_referenced_codelists,
    add_extensible_terms,
)
from cdisc_rules_engine.data_service.startup.populate_standards import (
    populate_standards,
)
from cdisc_rules_engine.data_service.startup.populate_dictionaries import (
    populate_dictionaries,
)
from cdisc_rules_engine.data_service.startup.populate_helper_tables import (
    populate_helper_tables,
)
from cdisc_rules_engine.models.dataset_metadata2 import (
    VariableMetadata,
)
from cdisc_rules_engine.models.test_dataset import TestDataset
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.utilities.ingestion_progress import IngestionProgressReporter
from cdisc_rules_engine.data_service.database import (
    DatabaseConfigPostgres,
    DatabaseConfigPGServer,
)

# Columns where dataset records can name the version of the CT package they use, by domain:
# (reference terminology column, CT version column). TS is the only known one so far
CT_VERSION_COLUMNS = {
    "TS": ("TSVCDREF", "TSVCDVER"),
}
# reference terminology values meaning CDISC CT, as in the non-SQL get_codelist_attributes operation
CDISC_CT_REFERENCES = ("CDISC", "CDISC CT")

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
        self.dictionary_metadata: Dict[str, Any] = {}
        self.cache_path: Optional[str] = None

    @classmethod
    def instance(
        cls,
        sql_namespace: Optional[str] = None,
        use_pgserver: bool = False,
        codelists: Optional[List[Union[str, Dict]]] = None,
        provided_codelists: Optional[List | str] = None,
        extensible_terms: Optional[Dict[str, Dict[str, Any]]] = None,
        external_dictionaries: Optional[SqlExternalDictionariesContainer] = None,
        cache_path: Optional[str] = None,
        define_xml_path: Optional[str] = None,
        stf_file_path: Optional[str] = None,
        progress_reporter: Optional[IngestionProgressReporter] = None,
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
        instance.dictionary_metadata = populate_dictionaries(
            pgi, external_dictionaries, progress_reporter=progress_reporter
        )
        instance.cache_path = cache_path
        populate_codelists(pgi, cache_path, codelists, progress_reporter=progress_reporter)
        populate_standards(pgi)
        populate_helper_tables(pgi)

        instance._update_define_xml_path(define_xml_path)
        instance._update_stf_file_path(stf_file_path)
        instance._update_provided_codelists(provided_codelists)
        instance._add_extensible_ct_terms(extensible_terms)

        return instance

    @classmethod
    def from_list_of_testdatasets(
        cls,
        test_datasets: list[TestDataset],
        standards_context: BaseStandardsContext,
        use_pgserver: bool = False,
        cache_path: Optional[str] = None,
        define_xml_path: Optional[str] = None,
        stf_file_path: Optional[str] = None,
    ) -> "PostgresQLDataService":
        """
        Constructor for tests, passing in TestDataset
        and create corresponding SQL tables
        """
        instance = cls.instance(
            use_pgserver=use_pgserver,
            cache_path=cache_path,
            define_xml_path=define_xml_path,
            stf_file_path=stf_file_path,
        )
        instance.datasets += [
            standards_context.transform_dataset_metadata(SqlTestDatasetLoader.load_test_dataset(instance.pgi, ds))
            for ds in test_datasets
        ]
        instance._populate_data_referenced_codelists(standards_context)
        SqlDataPreprocessor.run(instance, standards_context)
        return instance

    @classmethod
    def from_dataset_paths(
        cls,
        dataset_paths,
        standards_context,
        codelists: Optional[List[Union[str, Dict]]] = None,
        provided_codelists: Optional[List | str] = None,
        extensible_terms: Optional[Dict[str, Dict[str, Any]]] = None,
        external_dictionaries: Optional[SqlExternalDictionariesContainer] = None,
        cache_path: Optional[str] = None,
        define_xml_path: Optional[str] = None,
        stf_file_path: Optional[str] = None,
        sql_namespace: Optional[str] = None,
        use_pgserver: bool = False,
        progress_reporter: Optional[IngestionProgressReporter] = None,
    ) -> "PostgresQLDataService":
        instance = cls.instance(
            sql_namespace=sql_namespace,
            use_pgserver=use_pgserver,
            codelists=codelists,
            provided_codelists=provided_codelists,
            extensible_terms=extensible_terms,
            cache_path=cache_path,
            external_dictionaries=external_dictionaries,
            define_xml_path=define_xml_path,
            stf_file_path=stf_file_path,
            progress_reporter=progress_reporter,
        )

        instance.datasets.extend(
            standards_context.transform_dataset_metadata(ds)
            for ds in SqlDatasetLoader.load_datasets(instance.pgi, dataset_paths, progress_reporter=progress_reporter)
        )
        instance._populate_data_referenced_codelists(standards_context)
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

    def _update_stf_file_path(self, stf_file_path: str):
        self.stf_file_path = stf_file_path

    def _update_provided_codelists(self, provided_codelists: Optional[List | str] = None):
        self.provided_codelists = provided_codelists

    def _add_extensible_ct_terms(self, extensible_terms: Dict[str, dict]):
        add_extensible_terms(self.pgi, extensible_terms)

    def _populate_data_referenced_codelists(self, standards_context: "BaseStandardsContext"):
        """
        Once the datasets are loaded, and before any rule runs, completes the codelists table with:
        - the cached CT packages named in the CT_VERSION_COLUMNS (e.g. TSVCDVER), for records referencing CDISC CT
        - the most recent cached CT package, if none of the standard's type is loaded at this point
        """
        from cdisc_rules_engine.standards.adam_standards_context import AdamStandardsContext
        from cdisc_rules_engine.standards.sdtm_standards_context import SdtmStandardsContext

        if isinstance(standards_context, AdamStandardsContext):
            ct_type = "adam"
        elif isinstance(standards_context, SdtmStandardsContext):
            ct_type = "sdtm"
        else:
            return

        version_dates_by_column = defaultdict(set)
        for dataset in self.datasets:
            if dataset.domain not in CT_VERSION_COLUMNS:
                continue
            reference_var, version_var = CT_VERSION_COLUMNS[dataset.domain]
            reference_col = self.pgi.schema.get_column_hash(dataset.name, reference_var)
            version_col = self.pgi.schema.get_column_hash(dataset.name, version_var)
            if not reference_col or not version_col:
                continue
            self.pgi.execute_sql(
                f"SELECT DISTINCT TRIM(CAST({version_col} AS TEXT)) AS version "
                f"FROM {self.pgi.schema.get_table_hash(dataset.name)} "
                f"WHERE {reference_col} IN %s",
                (CDISC_CT_REFERENCES,),
            )
            version_dates_by_column[version_var].update(row["version"] for row in self.pgi.fetch_all())
        for version_var, version_dates in version_dates_by_column.items():
            populate_referenced_codelists(self.pgi, self.cache_path, ct_type, version_dates, version_var)
        populate_latest_codelist(self.pgi, self.cache_path, ct_type)
