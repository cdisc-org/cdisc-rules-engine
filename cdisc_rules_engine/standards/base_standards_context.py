from abc import ABC, abstractmethod
from typing import Any

from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2


class BaseStandardsContext(ABC):
    @abstractmethod
    def derive_domain(self, filename: str) -> str:
        pass

    @abstractmethod
    def get_domain_variables(self, domain: str):
        pass

    @abstractmethod
    def get_domain_label(self, domain: str) -> str:
        pass

    @abstractmethod
    def within_rule_scope(self, scope: dict[str, Any], metadata: DatasetMetadata2):
        pass
