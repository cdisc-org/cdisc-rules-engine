from typing import Any

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
    SQLDatasetMetadata,
)
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext


class DefaultStandardsContext(BaseStandardsContext):

    def derive_domain(self, filename: str):
        return filename.upper()

    def get_domain_variables(self, domain: str):
        return []

    def get_domain_label(self, domain: str):
        return ""

    def within_rule_scope(self, scope: dict[str, Any], metadata: DatasetMetadata2):
        # TODO: Should do a domain check
        return True

    def perform_merge(
        self,
        data_service: PostgresQLDataService,
        original: str,
        dataset_metadata: SQLDatasetMetadata,
        merge_spec: dict[str, Any],
        rule: dict,
    ) -> str:
        return self._do_join_merge(data_service, original=original, merge_spec=merge_spec)
