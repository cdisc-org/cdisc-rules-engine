from typing import Any

from cdisc_rules_engine.data_service.postgresql_data_service import (
    BaseDatasetMetadata,
    PostgresQLDataService,
)
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext


class DefaultStandardsContext(BaseStandardsContext):
    def transform_dataset_metadata(self, source: DatasetMetadata2) -> BaseDatasetMetadata:
        return BaseDatasetMetadata(**source.__dict__, domain=self.derive_domain(source.name))

    def derive_rdomain(self, name: str) -> str:
        """The default implementation can return a NOOP"""
        return name

    def replace_domain_code(self, dataset_metadata: BaseDatasetMetadata, variable: str) -> str:
        """The default implementation can return a NOOP"""
        return variable

    def derive_domain(self, filename: str):
        return filename.upper()

    def get_domain_metadata(self, domain: str) -> dict:
        return {}

    def get_domain_variables(self, domain: str):
        return []

    def get_model_metadata(self):
        return []

    def get_standard_metadata(self):
        return []

    def get_domain_label(self, domain: str):
        return ""

    def get_ct_packages(self):
        return []

    def get_model_variables(self, domain: str):
        return []

    def get_library_variables_metadata(self, dataset_metadata: BaseDatasetMetadata) -> list:
        return []

    def within_rule_scope(self, scope: dict[str, Any], metadata: DatasetMetadata2):
        # TODO: Should do a domain check
        return True, ""

    def perform_merge(
        self,
        data_service: PostgresQLDataService,
        original: str,
        dataset_metadata: BaseDatasetMetadata,
        merge_spec: dict[str, Any],
        rule: dict,
    ) -> str:
        return self._do_join_merge(data_service, original=original, merge_spec=merge_spec)
