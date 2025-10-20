from abc import ABC, abstractmethod
from typing import Any

from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
    SQLDatasetMetadata,
)
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

    @abstractmethod
    def perform_merge(
        self,
        data_service: PostgresQLDataService,
        original: str,
        dataset_metadata: SQLDatasetMetadata,
        merge_spec: dict[str, Any],
        rule: dict,
    ) -> str:
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
