from abc import ABC, abstractmethod
from typing import Any, List, Dict

from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    BaseDatasetMetadata,
    PostgresQLDataService,
)
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)


class BaseStandardsContext(ABC):
    @abstractmethod
    def transform_dataset_metadata(self, source: DatasetMetadata2) -> BaseDatasetMetadata:
        pass

    @abstractmethod
    def derive_rdomain(self, name: str) -> str:
        pass

    @abstractmethod
    def replace_domain_code(self, dataset_metadata: BaseDatasetMetadata, variable: str) -> str:
        pass

    @abstractmethod
    def get_domain_metadata(self, domain: str) -> dict:
        pass

    @abstractmethod
    def get_domain_variables(self, domain: str):
        pass

    @abstractmethod
    def get_model_metadata(self):
        pass

    @abstractmethod
    def get_standard_metadata(self):
        pass

    @abstractmethod
    def get_domain_label(self, domain: str) -> str:
        pass

    @abstractmethod
    def get_ct_packages(self):
        pass

    @abstractmethod
    def get_model_variables(self, domain: str):
        pass

    @abstractmethod
    def get_library_variables_metadata(self, dataset_metadata: BaseDatasetMetadata) -> list:
        pass

    @abstractmethod
    def within_rule_scope(self, scope: dict[str, Any], metadata: DatasetMetadata2):
        pass

    @abstractmethod
    def perform_merge(
        self,
        data_service: PostgresQLDataService,
        original: str,
        dataset_metadata: BaseDatasetMetadata,
        merge_spec: dict[str, Any],
        rule: dict,
    ) -> str:
        pass

    @abstractmethod
    def detect_split_datasets(self, dataset_names: List[str]) -> Dict[str, List[str]]:
        """
        Detect split datasets by naming convention.
        """
        pass

    def _do_join_merge(self, ds: PostgresQLDataService, original: str, merge_spec: dict) -> str:
        """
        Perform a join merge operation on the datasets.
        """
        right: str = merge_spec.get("domain_name").lower()
        join_type = merge_spec.get("join_type", "INNER")
        # For now we assume pivot columns are always the same in left and right
        pivot_columns = merge_spec.get("match_key", [])

        joined_schema = SqlJoinMerge.perform_join(
            pgi=ds.pgi,
            left=ds.pgi.schema.get_table(original),
            right=ds.pgi.schema.get_table(right),
            pivot_left=pivot_columns,
            pivot_right=pivot_columns,
            type=join_type.upper(),
        )

        return joined_schema.name

    def get_define_xml_variables_metadata(self, ds: PostgresQLDataService, domain_name: str) -> List[dict]:
        """
        Gets Define XML variables metadata.
        """

        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            ds.define_xml_path, ds.define_xml_path, ds, None
        )
        return define_xml_reader.extract_variables_metadata(domain_name=domain_name)
