from typing import List, Tuple

from cdisc_rules_engine.constants.classes import (
    EVENTS,
    FINDINGS,
    FINDINGS_ABOUT,
    INTERVENTIONS,
    RELATIONSHIP,
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
from cdisc_rules_engine.utilities.sdtm_utilities import get_class_and_domain_metadata
from cdisc_rules_engine.utilities.utils import (
    convert_library_class_name_to_ct_class,
    is_ap_domain,
    search_in_list_of_dicts,
)


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
        domain = self.derive_domain(metadata.filename)

        if not self.rule_applies_to_class(metadata, rule, domain):
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

    def rule_applies_to_class(self, dataset_metadata: SDTMDatasetMetadata, rule: dict, domain: str):
        """
        If included classes are specified and the class
        is not in the list of included classes return false.

        If excluded classes are specified and the class
        is in the list of excluded classes return false

        Else return true.

        Rule authors can specify classes to include that we cannot detect.
        In this case, the get_dataset_class method will return None,
        but included_classes will have values.
        This will result in a rule not running when it is supposed to.
        We filter out non-detectable classes here, so that rule authors
        can specify them without it affecting if the rule runs or not.
        """
        classes = rule.get("classes") or {}
        included_classes = classes.get("Include", [])
        excluded_classes = classes.get("Exclude", [])
        is_included = True
        is_excluded = False

        class_name = self.derive_class(dataset_metadata, domain)

        if included_classes:
            if ALL_KEYWORD in included_classes:
                return True
            if (class_name not in included_classes) and not (
                class_name == FINDINGS_ABOUT and FINDINGS in included_classes
            ):
                is_included = False

        if excluded_classes:
            if class_name and (
                (class_name in excluded_classes) or (class_name == FINDINGS_ABOUT and FINDINGS in excluded_classes)
            ):
                is_excluded = True
        return is_included and not is_excluded

    def derive_class(self, dataset_metadata: SDTMDatasetMetadata, domain: str):
        class_data, _ = get_class_and_domain_metadata(
            self.library_metadata.standard_metadata,
            domain,
        )
        name = class_data.get("name")
        if name:
            return convert_library_class_name_to_ct_class(name)
        else:
            return self._handle_special_cases(dataset_metadata)

    def _handle_special_cases(
        self,
        dataset_metadata: SDTMDatasetMetadata,
    ):
        if not dataset_metadata.domain:
            return None
        if self._contains_topic_variable(dataset_metadata, dataset_metadata.domain, "TERM"):
            return EVENTS
        if self._contains_topic_variable(dataset_metadata, dataset_metadata.domain, "TRT"):
            return INTERVENTIONS
        if self._contains_topic_variable(dataset_metadata, dataset_metadata.domain, "QNAM"):
            return RELATIONSHIP
        if self._contains_topic_variable(dataset_metadata, dataset_metadata.domain, "TESTCD"):
            if self._contains_topic_variable(dataset_metadata, dataset_metadata.domain, "OBJ"):
                return FINDINGS_ABOUT
            return FINDINGS
        if self._is_associated_persons(dataset_metadata):
            return self._get_associated_persons_inherit_class(dataset_metadata.domain)
        return None

    def _is_associated_persons(self, dataset) -> bool:
        """
        Check if AP-- domain.
        """
        return "APID" in dataset

    def _get_associated_persons_inherit_class(self, domain: str):
        """
        Find the domain this AP-- domain is related to, return its class.
        """
        # TODO: Needs access to other datasets, how will we do that?
        return None
        # ap_suffix = domain[2:]
        # directory_path = get_directory_path(file_path)
        # if len(datasets) > 1:
        #     domain_details: SDTMDatasetMetadata = search_in_list_of_dicts(
        #         datasets, lambda item: item.domain == ap_suffix
        #     )
        #     if domain_details:
        #         file_name = domain_details.filename
        #         new_file_path = os.path.join(directory_path, file_name)
        #         new_domain_dataset = self.get_dataset(dataset_name=new_file_path)
        #     else:
        #         raise ValueError("Filename for domain doesn't exist")
        #     if self._is_associated_persons(new_domain_dataset):
        #         raise ValueError("Nested Associated Persons domain reference")
        #     return self.get_dataset_class(
        #         new_domain_dataset,
        #         new_file_path,
        #         datasets,
        #         domain_details,
        #     )
        # else:
        #    return None

    def _contains_topic_variable(
        self,
        dataset: SDTMDatasetMetadata,
        domain: str,
        variable: str,
    ) -> bool:
        """
        Checks if the given dataset-class string ends with a particular variable string.
        """

        def check_presence(key):
            # TODO: Needs to wait until we have the variables in the metadata
            return True
            # if hasattr(dataset, "columns"):
            #     columns = dataset.columns
            #     if hasattr(columns, "tolist"):
            #         columns = columns.tolist()
            #     in_dataset = key in columns
            #     in_values = key in self.dataset_implementation.get_series_values(
            #         dataset
            #     )
            # else:
            #     series_values = dataset.values
            #     if hasattr(series_values, "tolist"):
            #         series_values = series_values.tolist()
            #     in_dataset = key in series_values
            #     in_values = key in self.dataset_implementation.get_series_values(
            #         dataset
            #     )
            # return in_dataset or in_values

        if not check_presence("DOMAIN") and not check_presence("RDOMAIN"):
            return False
        elif check_presence("DOMAIN"):
            return check_presence(domain.upper() + variable)
        elif check_presence("RDOMAIN"):
            return check_presence(variable)
