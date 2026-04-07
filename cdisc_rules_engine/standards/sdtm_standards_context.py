import re
from typing import Any, List, Tuple, Dict
from collections import defaultdict

from cdisc_rules_engine.constants.classes import (
    EVENTS,
    FINDINGS,
    FINDINGS_ABOUT,
    INTERVENTIONS,
    RELATIONSHIP,
    DETECTABLE_CLASSES,
)
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.data_service.merges.child import SqlChildMerge
from cdisc_rules_engine.data_service.merges.relationship import SqlRelationshipMerge
from cdisc_rules_engine.data_service.merges.relrec import SqlRelrecMerge
from cdisc_rules_engine.data_service.merges.supp import SqlSuppMerge
from cdisc_rules_engine.data_service.postgresql_data_service import (
    BaseDatasetMetadata,
    PostgresQLDataService,
)
from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.standards.sdtm_dataset_metadata import SdtmDatasetMetadata2
from cdisc_rules_engine.utilities.sdtm_utilities import (
    get_class_and_domain_metadata,
    get_class_metadata,
    get_allowed_class_variables,
    replace_variable_wildcards,
)
from cdisc_rules_engine.utilities.utils import (
    convert_library_class_name_to_ct_class,
    search_in_list_of_dicts,
)
from cdisc_rules_engine.utilities import sdtm_utilities


class SdtmStandardsContext(BaseStandardsContext):
    def __init__(self, library_metadata: LibraryMetadataContainer):
        super().__init__()
        self.library_metadata = library_metadata

    @property
    def standard(self):
        """Process standard metadata to get standard name"""
        return self.library_metadata.standard_metadata.get("name", "").split(" ")[0]

    def transform_dataset_metadata(self, source: DatasetMetadata2) -> SdtmDatasetMetadata2:
        domain = self.derive_domain(source.name)
        return SdtmDatasetMetadata2(
            **source.__dict__,
            domain=domain,
            is_supp=(domain.startswith("SUPP") or domain.startswith("SQ")),
            is_split=self.derive_is_split(source.name, domain),
            rdomain=self.derive_rdomain(source.name),
            domain_code=self.derive_domain_code(domain),
        )

    def derive_domain(self, filename: str):
        filename = filename.lower()

        # may need to consider FA and QS here?
        if filename.startswith("supp"):
            return filename[0:6].upper()
        elif filename.startswith("sq"):
            return filename[0:4].upper()
        elif filename.startswith("relrec"):
            return "RELREC"
        elif filename.startswith("relspec"):
            return "RELSPEC"
        elif filename.startswith("relsub"):
            return "RELSUB"
        else:
            return filename[0:2].upper()

    def derive_rdomain(self, name: str) -> str:
        if name.lower().startswith("supp"):
            return self.derive_domain(name[4:])
        elif name.lower().startswith("sq"):
            return self.derive_domain(name[2:])
        else:
            return ""

    def derive_domain_code(self, domain: str):
        """Derive the domain code for this domain"""
        if domain.startswith("SUPP") or domain.startswith("SQ"):
            return "Q"
        if domain in ["RELREC", "RELSPEC", "RELSUB"]:
            return ""
        else:
            return domain

    def derive_is_split(self, name: str, domain: str):
        if domain in ["SUPPQUAL", "RELREC", "RELSPEC", "RELSUB"]:
            return False
        elif name.lower().startswith("AP"):
            return len(name) > 4
        else:
            return len(name) > 2

    def replace_domain_code(self, dataset_metadata: SdtmDatasetMetadata2, variable: str) -> str:
        """Replace any -- with the domain code"""
        if "--" in variable and dataset_metadata.domain_code:
            return variable.replace("--", dataset_metadata.domain_code)
        return variable

    def get_domain_metadata(self, domain: str) -> dict:
        standard_data = self.get_standard_metadata()
        for c in standard_data.get("classes", []):
            domain_details = search_in_list_of_dicts(c.get("datasets", []), lambda item: item["name"] == domain)
            if domain_details:
                return domain_details
        # If not found, and domain is SUPP-- or SQ--, fall back to SUPPQUAL if it is present
        # Could be more efficiently done in a single passthrough, but will leave the rewrite until fully confirmed
        # wrt SUPP domain handling
        if domain.startswith("SUPP") or domain.startswith("SQ"):
            domain_details = search_in_list_of_dicts(c.get("datasets", []), lambda item: item["name"] == "SUPPQUAL")
            if domain_details:
                return domain_details
        return {}

    def get_domain_variables(self, domain: str):
        domain_details = self.get_domain_metadata(domain)
        if domain_details:
            variables_metadata = domain_details.get("datasetVariables", [])
            if variables_metadata:
                variables_metadata.sort(
                    key=lambda item: (
                        int(item.get("ordinal")) if item.get("ordinal") else int(item.get("order_number"))
                    )
                )
                return variables_metadata
        return []

    def get_model_metadata(self):
        model_metadata = self.library_metadata.model_metadata
        return model_metadata

    def get_standard_metadata(self):
        standard_metadata = self.library_metadata.standard_metadata
        return standard_metadata

    def get_domain_label(self, domain: str):
        domain_details = self.get_domain_metadata(domain)
        if domain_details:
            return domain_details.get("label", "")
        return ""

    def get_ct_packages(self):
        ct_packages = self.library_metadata.published_ct_packages
        return ct_packages

    def get_model_variables(self, domain: str, class_nm: str = None):
        # For SQL operations, use a simplified version that works with available metadata
        model_details = self.get_model_metadata()

        # Handle SUPP domain normalization like the original function
        if domain and (domain.upper().startswith("SUPP") or domain.upper().startswith("SQ")) and len(domain) > 2:
            domain = "SUPPQUAL"

        domain_details = sdtm_utilities.get_model_domain_metadata(model_details, domain)
        variables_metadata = []
        class_name = None

        if domain_details:
            # Domain found in the model
            class_name = convert_library_class_name_to_ct_class(domain_details["_links"]["parentClass"]["title"])
            class_details = sdtm_utilities.get_class_metadata(model_details, class_name)
            variables_metadata = domain_details.get("datasetVariables", [])
            if variables_metadata:
                variables_metadata.sort(key=lambda item: int(item["ordinal"]))
        else:
            # Domain not found in the model. Use the new get_dataset_class method
            class_name = class_nm

            if class_name is None:
                # Fall back to General Observations class for unknown domains
                from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS

                class_name = GENERAL_OBSERVATIONS_CLASS

            class_details = sdtm_utilities.get_class_metadata(model_details, class_name)

        # Apply class-specific logic for detectable classes
        from cdisc_rules_engine.constants.classes import DETECTABLE_CLASSES

        if class_name and class_name in DETECTABLE_CLASSES:
            (
                identifiers_metadata,
                class_variables_metadata,
                timing_metadata,
            ) = sdtm_utilities.get_allowed_class_variables(model_details, class_details)
            # Identifiers are added to the beginning and Timing to the end
            variables_metadata = class_variables_metadata
            if identifiers_metadata:
                variables_metadata = identifiers_metadata + variables_metadata
            if timing_metadata:
                variables_metadata = variables_metadata + timing_metadata

        return variables_metadata

    def get_library_variables_metadata(self, dataset_metadata: SdtmDatasetMetadata2) -> list:
        if not dataset_metadata.domain and dataset_metadata.is_supp and dataset_metadata.rdomain:
            domain = "SUPPQUAL"
        elif not dataset_metadata.domain and not dataset_metadata.rdomain and "rel" in dataset_metadata.name.lower():
            if dataset_metadata.name.lower().startswith("ap") and dataset_metadata.name.lower()[2:].startswith("rel"):
                domain = dataset_metadata.name[2:]
            else:
                domain = dataset_metadata.name
        else:
            domain = dataset_metadata.domain

        derived_class = self.derive_class(dataset_metadata, domain)

        variables = self.get_variables_metadata_from_standard(
            domain=domain, library_metadata=self.library_metadata, derived_class=derived_class
        )

        column_name_mapping = {
            "ordinal": "order_number",
            "simpleDatatype": "data_type",
        }

        for var in variables:
            # Replace -- with domain code if it exists
            var["name"] = var["name"].replace("--", dataset_metadata.domain or "")
            for key, new_key in column_name_mapping.items():
                if key in var:
                    var[new_key] = var.pop(key)

        return variables

    def get_variables_metadata_from_standard(self, domain, library_metadata, derived_class=None):  # noqa
        standard_details = library_metadata.standard_metadata
        model_details = library_metadata.model_metadata
        is_custom = domain not in standard_details.get("domains", {})
        variables_metadata = []
        IG_class_details, IG_domain_details = get_class_and_domain_metadata(standard_details, domain)
        if IG_class_details:
            class_name = convert_library_class_name_to_ct_class(IG_class_details.get("name"))
        else:
            class_name = derived_class
        IG_domain_details = self._ig_domain_details_standardisation(IG_domain_details)
        model_class_details = get_class_metadata(model_details, class_name)
        # Both custom and standard General Observations pull from model
        if is_custom or class_name in DETECTABLE_CLASSES:
            (
                identifiers_metadata,
                class_variables_metadata,
                timing_metadata,
            ) = get_allowed_class_variables(model_details, model_class_details)
            model_variables = []
            for var_list in [
                identifiers_metadata,
                class_variables_metadata,
                timing_metadata,
            ]:
                replace_variable_wildcards(var_list, domain, model_variables)
        # Custom domains only pull from model hierarchy
        if is_custom:
            variables_metadata = model_variables
        # All non-custom domains pull from IG and overwrite the model variables
        else:
            ig_variables = IG_domain_details.get("datasetVariables", [])
            ig_variables.sort(key=lambda item: int(item["ordinal"]))
            if class_name in DETECTABLE_CLASSES:
                variables_metadata = model_variables.copy()
                model_vars_by_name = {var["name"]: i for i, var in enumerate(variables_metadata)}
                for ig_var in ig_variables:
                    ig_var_name = ig_var["name"]
                    if ig_var_name in model_vars_by_name:
                        variables_metadata[model_vars_by_name[ig_var_name]] = ig_var
                    else:
                        # if a variable exists in the IG but not in the model,
                        # insert it at the end of the its section
                        ig_var_role = ig_var.get("role")
                        if ig_var_role == "Identifier":
                            identifiers_length = len(identifiers_metadata)
                            insertion_point = identifiers_length
                        elif ig_var_role == "Timing":
                            insertion_point = len(variables_metadata)
                        else:
                            timing_metadata_length = len(timing_metadata)
                            insertion_point = len(variables_metadata) - timing_metadata_length
                        variables_metadata.insert(insertion_point, ig_var)
                        model_vars_by_name = {var["name"]: i for i, var in enumerate(variables_metadata)}
            else:
                variables_metadata = ig_variables
        return variables_metadata

    def within_rule_scope(self, rule: dict, metadata: DatasetMetadata2):
        """Check if rule is suitable and return reason if not"""
        rule_id = rule.get("core_id", "unknown")
        dataset_name = metadata.name
        domain = self.derive_domain(metadata.name)
        is_split = self.derive_is_split(metadata.name, domain)

        if not self.rule_applies_to_class(metadata, rule, domain):
            reason = f"Rule skipped - doesn't apply to class for " f"rule id={rule_id}, dataset={dataset_name}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason
        if not self.rule_applies_to_domain(metadata, rule, domain, is_split=is_split):
            reason = f"Rule skipped - doesn't apply to domain for rule id={rule_id}, dataset={dataset_name}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason

        logger.info(f"is_suitable_for_validation. rule id={rule_id}, dataset={dataset_name}, result=True")
        return True, ""

    def perform_merge(
        self,
        data_service: PostgresQLDataService,
        original: str,
        dataset_metadata: BaseDatasetMetadata,
        merge_spec: dict[str, Any],
        rule: dict,
    ) -> str:
        right: str = merge_spec.get("domain_name").lower()
        is_relationship = merge_spec.get("relationship_columns", None) is not None
        is_child = bool(merge_spec.get("child"))

        if is_child:
            # TODO: This logic should be unnecessary, why is the same rule testing so many things
            # Gate: Only merge if domain_name matches current dataset
            domain_name = merge_spec.get("domain_name")

            is_general_supp_merge = domain_name.startswith("SUPP") and dataset_metadata.is_supp
            domain_matches = domain_name.upper() == dataset_metadata.domain.upper()

            if is_general_supp_merge or domain_matches:
                return self._do_child_merge(
                    data_service,
                    child=original,
                    dataset_metadata=dataset_metadata,
                    merge_spec=merge_spec,
                    rule=rule,
                )
            else:
                # This should really throw an error, because why are there merges defined which don't do
                # anything, but this would break CORE RULE 206 currently
                return original
        elif right == "relrec":
            return self._do_relrec_merge(
                data_service,
                original=original,
                relrec_dataset=right,
                dataset_metadata=dataset_metadata,
                merge_spec=merge_spec,
                rule=rule,
            )
        elif right == "supp--":
            return self._do_supp_merge(
                data_service,
                original=original,
                target=right,
                dataset_metadata=dataset_metadata,
                merge_spec=merge_spec,
                rule=rule,
            )
        elif is_relationship:
            return self._do_relationship_merge(
                data_service,
                original=original,
                relationship_dataset=right,
                dataset_metadata=dataset_metadata,
                merge_spec=merge_spec,
                rule=rule,
            )
        else:
            return self._do_join_merge(data_service, original=original, merge_spec=merge_spec)

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
        is_excluded, is_included = cls._handle_split_domains(
            is_split,
            include_split_datasets,
            is_excluded,
            is_included,
        )

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
         and the domain is not in the list of included domains,
         and domain doesn't match with AP / APFA / APRELSUB / SUPP / SQ naming pattern
         -> domain is not included.
        In other cases domain is included
        """
        if not included_domains:
            if include_split_datasets is True and not is_split:
                return False
            return True

        if domain in included_domains or dataset_metadata.name in included_domains or ALL_KEYWORD in included_domains:
            return True
        if cls._domain_matched_ap_or_supp(dataset_metadata, domain, included_domains):
            return True
        return False

    @classmethod
    def _is_domain_name_excluded(
        cls, dataset_metadata: DatasetMetadata2, domain: str, excluded_domains: List[str]
    ) -> bool:
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
            domain in excluded_domains
            or dataset_metadata.name in excluded_domains
            # or dataset_metadata.unsplit_name in excluded_domains
            or ALL_KEYWORD in excluded_domains
        ):
            return True
        if cls._domain_matched_ap_or_supp(dataset_metadata, domain, excluded_domains):
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
    def _domain_matched_ap_or_supp(
        cls, dataset_metadata: DatasetMetadata2, domain: str, domains_to_check: List[str]
    ) -> bool:
        """
        Check that domain name match with only
        AP / APFA / APRELSUB / SUPP / SQ naming pattern
        """
        # supp_ap_domains = {f"{domain}--" for domain in SUPPLEMENTARY_DOMAINS}
        # supp_ap_domains.update({f"{AP_DOMAIN}--", f"{APFA_DOMAIN}--"})

        # return any(set(domains_to_check).intersection(supp_ap_domains)) and (
        #     domain == "SUPPQUAL"
        #     or is_ap_domain(dataset_metadata.domain or dataset_metadata.rdomain or dataset_metadata.name)
        # )
        if "SUPP--" in domains_to_check or "SQ--" in domains_to_check:
            if domain[0:4] == "SUPP" or domain[0:2] == "SQ":
                return True
        if "AP--" in domains_to_check or "APFA--" in domains_to_check:
            if domain == "AP":
                return True
        return False

    def rule_applies_to_class(self, dataset_metadata: DatasetMetadata2, rule: dict, domain: str):
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

    def derive_class(self, dataset_metadata: DatasetMetadata2, domain: str):
        class_data, _ = get_class_and_domain_metadata(
            self.library_metadata.standard_metadata,
            domain,
        )
        name = class_data.get("name")
        if name:
            return convert_library_class_name_to_ct_class(name)
        else:
            return self._handle_special_cases(dataset_metadata, domain)

    def _handle_special_cases(self, dataset_metadata: DatasetMetadata2, domain: str):
        if not domain:
            return None
        if self._contains_topic_variable(dataset_metadata, domain, "TERM"):
            return EVENTS
        if self._contains_topic_variable(dataset_metadata, domain, "TRT"):
            return INTERVENTIONS
        if self._contains_topic_variable(dataset_metadata, domain, "QNAM"):
            return RELATIONSHIP
        if self._contains_topic_variable(dataset_metadata, domain, "TESTCD"):
            if self._contains_topic_variable(dataset_metadata, domain, "OBJ"):
                return FINDINGS_ABOUT
            return FINDINGS
        # if self._is_associated_persons(dataset_metadata):
        if domain == "AP":
            return self._get_associated_persons_inherit_class(domain)
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
        dataset: DatasetMetadata2,
        domain: str,
        variable: str,
    ) -> bool:
        """
        Checks if the given dataset-class string ends with a particular variable string.
        """

        def check_presence(key):
            return any(v for v in dataset.variables if v.name.lower() == key.lower())

        if not check_presence("DOMAIN") and not check_presence("RDOMAIN"):
            return False
        elif check_presence("DOMAIN"):
            return check_presence(domain.upper() + variable)
        elif check_presence("RDOMAIN"):
            return check_presence(variable)

    def _do_supp_merge(
        self,
        data_service: PostgresQLDataService,
        original: str,
        target: str,
        dataset_metadata: SdtmDatasetMetadata2,
        merge_spec: dict,
        rule: dict,
    ) -> str:
        """
        Find the corresponding SUPP datasets, then perform a SUPP merge operation on the datasets.
        """
        rdomain = dataset_metadata.rdomain
        if target != "supp--" and rdomain not in target:
            raise ValueError(f"Tried to SUPP merge {rdomain}, but the target domain {target} does not match.")

        supp_dataset = next(
            (dataset for dataset in data_service.datasets if dataset.name.lower() == f"supp--{rdomain}"),
            None,
        )
        if not supp_dataset:
            raise ValueError(f"Tried to SUPP merge {rdomain}, but could not find corresponding SUPP dataset.")

        return SqlSuppMerge.perform_join(
            pgi=data_service.pgi,
            original=data_service.pgi.schema.get_table(original),
            supp=data_service.pgi.schema.get_table(supp_dataset.name),
            domain=rdomain,
        ).name

    def _do_relrec_merge(
        self,
        data_service: PostgresQLDataService,
        original: str,
        relrec_dataset: str,
        dataset_metadata: BaseDatasetMetadata,
        merge_spec: dict,
        rule: dict,
    ) -> str:
        """
        Find the corresponding RELREC dataset, then perform a RELREC merge operation on the datasets.
        """
        # Find the RELREC dataset
        relrec_data = next(
            (dataset for dataset in data_service.datasets if self.derive_domain(dataset.name) == "RELREC"),
            None,
        )
        if not relrec_data:
            raise ValueError("Tried to RELREC merge, but could not find RELREC dataset.")

        wildcard = merge_spec.get("wildcard", "__")

        return SqlRelrecMerge.perform_join(
            pgi=data_service.pgi,
            original=data_service.pgi.schema.get_table(original),
            relrec=data_service.pgi.schema.get_table(relrec_data.name),
            domain=dataset_metadata.domain,
            wildcard=wildcard,
        ).name

    def _do_relationship_merge(
        self,
        data_service: PostgresQLDataService,
        original: str,
        relationship_dataset: str,
        dataset_metadata: SdtmDatasetMetadata2,
        merge_spec: dict,
        rule: dict,
    ) -> str:
        """
        Perform a relationship merge operation on the datasets.

        This handles relationship datasets like RELSUB, CO, SQ, or any dataset with relationship_columns.
        """
        # Find the relationship dataset
        domain = dataset_metadata.domain
        relationship_data = next(
            (
                dataset
                for dataset in data_service.datasets
                if dataset.name.upper() == relationship_dataset.upper() or (domain == relationship_dataset.upper())
            ),
            None,
        )
        if not relationship_data:
            raise ValueError(f"Tried to relationship merge with {relationship_dataset}, but could not find dataset.")

        relationship_columns = merge_spec.get("relationship_columns", {})
        match_keys = merge_spec.get("match_key", {})

        return SqlRelationshipMerge.perform_join(
            pgi=data_service.pgi,
            original=data_service.pgi.schema.get_table(original),
            relationship_dataset=data_service.pgi.schema.get_table(relationship_data.name),
            domain=relationship_dataset.upper(),
            relationship_columns=relationship_columns,
            match_keys=match_keys,
        ).name

    def _do_child_merge(
        self,
        data_service: PostgresQLDataService,
        child: str,
        dataset_metadata: BaseDatasetMetadata,
        merge_spec: dict,
        rule: dict,
    ) -> str:
        """
        Perform child merge: Find parent dataset and LEFT JOIN child with parent.

        Child dataset is on the left, parent on the right.
        Uses SqlChildMerge for the operation.
        """
        result_schema = SqlChildMerge.perform_merge(
            pgi=data_service.pgi,
            child=data_service.pgi.schema.get_table(child),
            child_domain=dataset_metadata.domain,
            datasets=data_service.datasets,
            merge_spec=merge_spec,
        )
        return result_schema.name

    def detect_split_datasets(self, dataset_names: List[str]) -> Dict[str, List[str]]:
        """
        Detect split datasets by name.
        """
        split_groups = defaultdict(list)

        datasets = [name.lower() for name in dataset_names]

        for dataset in datasets:
            unsplit_name = self._get_unsplit_name(dataset)

            if unsplit_name != dataset and unsplit_name not in datasets:
                split_groups[unsplit_name].append(dataset)

        return {k: v for k, v in split_groups.items() if len(v) > 1}

    @staticmethod
    def _get_unsplit_name(dataset_name: str) -> str:
        """
        Extract the unsplit (logical) name from a dataset name following
        SDTMIG v3.4 naming conventions.
        """
        dataset = dataset_name.lower()

        # suppfa + parent domain (e.g., suppfacm -> suppfa)
        if dataset.startswith("suppfa") and len(dataset) > 6:
            return "suppfa"

        # fa + parent domain (e.g., facm, faeg -> fa)
        if dataset.startswith("fa") and len(dataset) == 4:
            return "fa"

        # supp + parent domain + alphanumeric suffix (e.g., suppae1 -> suppae)
        if dataset.startswith("supp") and len(dataset) > 4:
            match = re.match(r"^(supp[a-z]{2})([a-z0-9]+)$", dataset)
            if match:
                return match.group(1)

        # relrec + alphanumeric suffix (e.g., relreca -> relrecb)
        if dataset.startswith("relrec") and len(dataset) > 6:
            return "relrec"

        # 2-char parent domain + alphanumeric suffix (e.g., ae1 -> ae)
        if len(dataset) > 2:
            match = re.match(r"^([a-z]{2})([a-z0-9]+)$", dataset)
            if match:
                return match.group(1)

        return dataset

    @staticmethod
    def _ig_domain_details_standardisation(
        ig_domain_details: LibraryMetadataContainer,
    ) -> LibraryMetadataContainer:
        """
        rename keys in ig_domain_details to avoid key reference issues
        """

        if not ig_domain_details:
            return ig_domain_details

        for i, e in enumerate(ig_domain_details.get("datasetVariables", {})):
            # TODO get exhausive list of keys to rename and standardise this process with a mapping dict / constants
            e = {"ordinal" if k == "order_number" else k: v for k, v in e.items()}
            e = {"ordinal" if k == "library_variable_order_number" else k: v for k, v in e.items()}
            e = {"name" if k == "library_variable_name" else k: v for k, v in e.items()}
            ig_domain_details["datasetVariables"][i] = e

        return ig_domain_details
