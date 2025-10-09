from typing import Any

from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext


class DefaultStandardsContext(BaseStandardsContext):

    def derive_domain(self, filename: str):
        return filename.upper()

    def get_domain_variables(self, domain: str):
        return []

    def get_domain_label(self, domain: str):
        return ""

    def within_rule_scope(self, scope: dict[str, Any], metadata: DatasetMetadata):
        # TODO: Should do a domain check
        return True
