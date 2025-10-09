from typing import List, Optional, Tuple

from cdisc_rules_engine.constants.classes import (
    ASSOCIATED_PERSONS,
    EVENTS,
    FINDINGS,
    FINDINGS_ABOUT,
    INTERVENTIONS,
    RELATIONSHIP,
    SPECIAL_PURPOSE,
    TRIAL_DESIGN,
)
from cdisc_rules_engine.constants.domains import (
    AP_DOMAIN,
    APFA_DOMAIN,
    SUPPLEMENTARY_DOMAINS,
)
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.utilities.utils import is_ap_domain, search_in_list_of_dicts


class SdtmStandardsContext(BaseStandardsContext):
    def __init__(self, library_metadata: LibraryMetadataContainer):
        super().__init__()
        self.library_metadata = library_metadata

    def derive_domain(self, filename: str):
        filename = filename.lower()

        if filename.startswith("supp") or filename.startswith("sq"):
            return "SUPPQUAL"
        elif filename.startswith("relrec"):
            return "RELREC"
        elif filename.startswith("relspec"):
            return "RELSPEC"
        elif filename.startswith("relsub"):
            return "RELSUB"
        else:
            return filename[0:2].upper()

    def get_domain_variables(self, domain: str):
        # TODO: Fetch from metadata
        return []

    def get_domain_label(self, domain: str):
        standard_data = self.library_metadata.standard_metadata
        for c in standard_data.get("classes", []):
            domain_details = search_in_list_of_dicts(c.get("datasets", []), lambda item: item["name"] == domain)
            if domain_details:
                return domain_details.get("label", "")
        return ""

    # TODO: Replace SDTMDatasetMetadata with a more generic metadata container
    def within_rule_scope(self, rule: dict, metadata: SDTMDatasetMetadata):
        """Check if rule is suitable and return reason if not"""
        rule_id = rule.get("core_id", "unknown")
        dataset_name = metadata.name

        if not self.rule_applies_to_class(metadata, rule):
            reason = f"Rule skipped - doesn't apply to class for " f"rule id={rule_id}, dataset={dataset_name}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason
        if not self.rule_applies_to_domain(metadata, rule):
            reason = f"Rule skipped - doesn't apply to domain for rule id={rule_id}, dataset={dataset_name}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason

        logger.info(f"is_suitable_for_validation. rule id={rule_id}, dataset={dataset_name}, result=True")
        return True, ""

    @classmethod
    def rule_applies_to_domain(cls, dataset_metadata: SDTMDatasetMetadata, rule: dict) -> bool:
        """
        Check that rule is applicable to dataset domain
        """
        domains = rule.get("domains") or {}
        include_split_datasets: bool = domains.get("include_split_datasets")

        included_domains = domains.get("Include", [])
        excluded_domains = domains.get("Exclude", [])

        is_included = cls._is_domain_name_included(dataset_metadata, included_domains, include_split_datasets)
        is_excluded = cls._is_domain_name_excluded(dataset_metadata, excluded_domains)

        # additional check for split domains based on the flag
        is_excluded, is_included = cls._handle_split_domains(
            dataset_metadata.is_split,
            include_split_datasets,
            is_excluded,
            is_included,
        )

        return is_included and not is_excluded

    @classmethod
    def _is_domain_name_included(
        cls,
        dataset_metadata: SDTMDatasetMetadata,
        included_domains: List[str],
        include_split_datasets: bool,
    ) -> bool:
        """
        If included domains aren't specified
         and include_split_datasets is True,
         and it is not a split dataset
         -> domain is not included
        If included domains are specified,
         and the domain is not in the list of included domains,
         and domain doesn't match with AP / APFA / APRELSUB / SUPP / SQ naming pattern
         -> domain is not included.
        In other cases domain is included
        """
        if not included_domains:
            if include_split_datasets is True and not dataset_metadata.is_split:
                return False
            return True

        if (
            dataset_metadata.domain in included_domains
            or dataset_metadata.name in included_domains
            or ALL_KEYWORD in included_domains
        ):
            return True
        if cls._domain_matched_ap_or_supp(dataset_metadata, included_domains):
            return True
        return False

    @classmethod
    def _is_domain_name_excluded(cls, dataset_metadata: SDTMDatasetMetadata, excluded_domains: List[str]) -> bool:
        """
        If excluded domains are specified,
         and the domain is in the list of excluded domains,
         or domain name match with AP / APFA / APRELSUB / SUPP / SQ naming pattern
         domain is excluded.

        In other cases domain is not excluded.
        """
        if not excluded_domains:
            return False

        if (
            dataset_metadata.domain in excluded_domains
            or dataset_metadata.name in excluded_domains
            or dataset_metadata.unsplit_name in excluded_domains
            or ALL_KEYWORD in excluded_domains
        ):
            return True
        if cls._domain_matched_ap_or_supp(dataset_metadata, excluded_domains):
            return True
        return False

    @classmethod
    def _handle_split_domains(
        cls,
        is_split_domain: bool,
        include_split_datasets: bool,
        is_excluded: bool,
        is_included: bool,
    ) -> Tuple[bool, bool]:
        """
        HANDLING SPLIT DOMAINS

        If include_split_datasets is True -
        add split domains to the list of included domains.
        If no included domains specified, only validate split domains

        If include_split_datasets is False - Exclude split domains
        If include_split_datasets is None - Do nothing
        """
        if include_split_datasets is True and is_split_domain and not is_excluded:
            is_included = True
        if include_split_datasets is False and is_split_domain:
            is_excluded = True
        return is_excluded, is_included

    @classmethod
    def _domain_matched_ap_or_supp(cls, dataset_metadata: SDTMDatasetMetadata, domains_to_check: List[str]) -> bool:
        """
        Check that domain name match with only
        AP / APFA / APRELSUB / SUPP / SQ naming pattern
        """
        supp_ap_domains = {f"{domain}--" for domain in SUPPLEMENTARY_DOMAINS}
        supp_ap_domains.update({f"{AP_DOMAIN}--", f"{APFA_DOMAIN}--"})

        return any(set(domains_to_check).intersection(supp_ap_domains)) and (
            dataset_metadata.is_supp
            or is_ap_domain(dataset_metadata.domain or dataset_metadata.rdomain or dataset_metadata.name)
        )

    @classmethod
    def rule_applies_to_class(cls, dataset_metadata: SDTMDatasetMetadata, rule: dict) -> bool:
        """Check if rule applies to dataset's class"""
        classes = rule.get("classes") or {}
        included_classes = classes.get("Include", [])
        excluded_classes = classes.get("Exclude", [])

        if not included_classes and not excluded_classes:
            return True

        # TODO: Fix
        variables = []  # dataset_metadata.variables if hasattr(dataset_metadata, "variables") else []
        domain_name = dataset_metadata.name
        dataset_class = cls.get_dataset_class_from_variables(variables, domain_name)

        if dataset_class is None and (included_classes or excluded_classes):
            logger.debug(
                f"Could not determine class for {domain_name}, variables: {variables if variables else 'none'}"
            )
        else:
            logger.debug(f"Dataset {domain_name} identified as class: {dataset_class}")

        if cls.matches_class_pattern(dataset_class, excluded_classes):
            return False

        if included_classes:
            return cls.matches_class_pattern(dataset_class, included_classes)

        return True

    @staticmethod
    def matches_class_pattern(dataset_class: Optional[str], patterns: list) -> bool:
        """Check if dataset class matches any patterns."""
        if dataset_class is None:
            return False

        for pattern in patterns:
            if pattern == ALL_KEYWORD:
                return True
            if pattern == dataset_class:
                return True
            if dataset_class == FINDINGS_ABOUT and pattern == FINDINGS:
                return True
        return False

    @classmethod
    def get_dataset_class_from_variables(cls, variables: List[str], domain_name: str) -> Optional[str]:
        """Determine dataset class based on variable names and domain"""
        variables_upper = [v.upper() for v in variables] if variables else []
        domain_upper = domain_name.upper()

        if domain_upper in ["DM", "CO", "SE", "SU", "SV", "SM"]:
            return SPECIAL_PURPOSE

        if domain_upper in ["TA", "TE", "TI", "TS", "TV"]:
            return TRIAL_DESIGN

        if any([domain_upper.startswith(prefix) for prefix in ["REL", "SUPP", "SQ"]]):
            return RELATIONSHIP

        if not variables:
            return cls.get_class_from_empty_variables(domain_upper)

        if any(
            v.endswith("TESTCD")
            or v.endswith("TEST")
            or v.endswith("ORRES")
            or v.endswith("STRESC")
            or v.endswith("STRESN")
            for v in variables_upper
        ):
            if any(v.endswith("OBJ") for v in variables_upper):
                return FINDINGS_ABOUT
            return FINDINGS

        if any(
            v.endswith("TRT") or v.endswith("DOSE") or v.endswith("DOSFRQ") or v.endswith("ROUTE")
            for v in variables_upper
        ):
            return INTERVENTIONS

        if any(
            v.endswith("TERM") or v.endswith("DECOD") or v.endswith("LLT") or v.endswith("PTCD")
            for v in variables_upper
        ):
            return EVENTS

        return None

    @staticmethod
    def get_class_from_empty_variables(domain_upper: str) -> Optional[str]:
        """Determine dataset class based on domain name when no variables are present."""
        if domain_upper in ["AE", "CE", "DS", "MH", "HO", "DV", "DD"]:
            return EVENTS
        if domain_upper in ["CM", "EX", "PR", "EC", "AG", "ML", "DO"]:
            return INTERVENTIONS
        if domain_upper in [
            "LB",
            "VS",
            "EG",
            "PE",
            "IE",
            "QS",
            "SC",
            "PC",
            "PP",
            "MB",
            "MS",
            "MI",
            "DA",
            "FT",
            "GF",
            "NV",
            "OE",
            "PK",
            "RS",
            "SS",
            "TR",
            "TU",
            "UR",
        ]:
            return FINDINGS
        if domain_upper == "FA":
            return FINDINGS_ABOUT
        if domain_upper.startswith("AP"):
            return ASSOCIATED_PERSONS
        return None
