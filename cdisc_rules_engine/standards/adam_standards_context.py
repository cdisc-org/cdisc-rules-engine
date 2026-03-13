from typing import List

from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.models.library_metadata_container import LibraryMetadataContainer
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.standards.adam_dataset_metadata import AdamDatasetMetadata2
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext


class AdamStandardsContext(DefaultStandardsContext):
    def __init__(self, library_metadata: LibraryMetadataContainer):
        super().__init__()
        self.library_metadata = library_metadata

    def transform_dataset_metadata(self, source: DatasetMetadata2) -> AdamDatasetMetadata2:
        domain = self.derive_domain(source.name)
        return AdamDatasetMetadata2(**source.__dict__, domain=domain)

    def derive_domain(self, filename: str):
        filename = filename.lower()
        return filename

    def within_rule_scope(self, rule: dict, metadata: DatasetMetadata2):
        """Check if rule is suitable and return reason if not"""
        rule_id = rule.get("core_id", "unknown")
        dataset_name = metadata.name
        domain = self.derive_domain(metadata.name)

        # TODO: ADaM classes
        # if not self.rule_applies_to_class(metadata, rule, domain):
        #     reason = f"Rule skipped - doesn't apply to class for " f"rule id={rule_id}, dataset={dataset_name}"
        #     logger.info(f"is_suitable_for_validation. {reason}, result=False")
        #     return False, reason
        if not self.rule_applies_to_domain(metadata, rule, domain, is_split=False):
            reason = f"Rule skipped - doesn't apply to domain for rule id={rule_id}, dataset={dataset_name}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason

        logger.info(f"is_suitable_for_validation. rule id={rule_id}, dataset={dataset_name}, result=True")
        return True, ""

    @classmethod
    def rule_applies_to_domain(
        cls, dataset_metadata: DatasetMetadata2, rule: dict, domain: str, is_split: bool
    ) -> bool:
        """
        Check that rule is applicable to dataset domain
        """
        domains = rule.get("domains") or {}
        include_split_datasets: bool = domains.get("include_split_datasets")

        included_domains = domains.get("Include", [])
        excluded_domains = domains.get("Exclude", [])

        is_included = cls._is_domain_name_included(
            dataset_metadata, domain, included_domains, include_split_datasets, is_split
        )
        is_excluded = cls._is_domain_name_excluded(dataset_metadata, domain, excluded_domains)

        # additional check for split domains based on the flag
        # is_excluded, is_included = cls._handle_split_domains(
        #     is_split,
        #     include_split_datasets,
        #     is_excluded,
        #     is_included,
        # )

        return is_included and not is_excluded

    @classmethod
    def _is_domain_name_included(
        cls,
        dataset_metadata: DatasetMetadata2,
        domain: str,
        included_domains: List[str],
        include_split_datasets: bool,
        is_split: bool,
    ) -> bool:
        """
        If included domains aren't specified
         and include_split_datasets is True,
         and it is not a split dataset
         -> domain is not included
        If included domains are specified,
         and the domain is not in the list of included domains
         -> domain is not included.
        In other cases domain is included
        """
        if not included_domains:
            if include_split_datasets is True and not is_split:
                return False
            return True

        included_domains = [domain.lower() for domain in included_domains]

        if domain in included_domains or dataset_metadata.name in included_domains or ALL_KEYWORD in included_domains:
            return True
        return False

    @classmethod
    def _is_domain_name_excluded(
        cls, dataset_metadata: DatasetMetadata2, domain: str, excluded_domains: List[str]
    ) -> bool:
        """
        If excluded domains are specified,
         and the domain is in the list of excluded domains,
         domain is excluded.

        In other cases domain is not excluded.
        """
        if not excluded_domains:
            return False

        excluded_domains = [domain.lower() for domain in excluded_domains]

        if (
            domain in excluded_domains
            or dataset_metadata.name in excluded_domains
            # or dataset_metadata.unsplit_name in excluded_domains
            or ALL_KEYWORD in excluded_domains
        ):
            return True
        return False
