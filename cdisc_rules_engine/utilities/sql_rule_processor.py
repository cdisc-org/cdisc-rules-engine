from typing import List, Optional, Tuple

from cdisc_rules_engine.config import config as default_config

# import os
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

# from cdisc_rules_engine.constants.domains import (
#     AP_DOMAIN,
#     APFA_DOMAIN,
#     SUPPLEMENTARY_DOMAINS,
# )
from cdisc_rules_engine.constants.domains import (
    AP_DOMAIN,
    APFA_DOMAIN,
    SUPPLEMENTARY_DOMAINS,
)
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
    SQLDatasetMetadata,
)
from cdisc_rules_engine.interfaces import ConditionInterface
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

# from cdisc_rules_engine.models.operation_params import OperationParams
# from cdisc_rules_engine.models.rule_conditions import AllowedConditionsKeys
# from cdisc_rules_engine.operations import operations_factory
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from cdisc_rules_engine.utilities.utils import is_ap_domain

# from cdisc_rules_engine.utilities.data_processor import DataProcessor
# from cdisc_rules_engine.utilities.utils import (
#     get_directory_path,
#     get_operations_cache_key,
#     is_ap_domain,
#     search_in_list_of_dicts,
#     get_dataset_name_from_details,
# )

# from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundError


class SQLRuleProcessor:
    def __init__(
        self,
        library_metadata: LibraryMetadataContainer = None,
    ):
        self.cache = CacheServiceFactory(default_config).get_cache_service()
        self.library_metadata = library_metadata

    @classmethod
    def rule_applies_to_domain(cls, dataset_metadata: SQLDatasetMetadata, rule: dict) -> bool:
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
        dataset_metadata: SQLDatasetMetadata,
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
            or dataset_metadata.dataset_name in included_domains
            or ALL_KEYWORD in included_domains
        ):
            return True
        if cls._domain_matched_ap_or_supp(dataset_metadata, included_domains):
            return True
        return False

    @classmethod
    def _is_domain_name_excluded(cls, dataset_metadata: SQLDatasetMetadata, excluded_domains: List[str]) -> bool:
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
            or dataset_metadata.dataset_name in excluded_domains
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
    def _domain_matched_ap_or_supp(cls, dataset_metadata: SQLDatasetMetadata, domains_to_check: List[str]) -> bool:
        """
        Check that domain name match with only
        AP / APFA / APRELSUB / SUPP / SQ naming pattern
        """
        supp_ap_domains = {f"{domain}--" for domain in SUPPLEMENTARY_DOMAINS}
        supp_ap_domains.update({f"{AP_DOMAIN}--", f"{APFA_DOMAIN}--"})

        return any(set(domains_to_check).intersection(supp_ap_domains)) and (
            dataset_metadata.is_supp
            or is_ap_domain(dataset_metadata.domain or dataset_metadata.rdomain or dataset_metadata.dataset_name)
        )

    @classmethod
    def rule_applies_to_class(cls, dataset_metadata: SQLDatasetMetadata, rule: dict) -> bool:
        """Check if rule applies to dataset's class"""
        classes = rule.get("classes") or {}
        included_classes = classes.get("Include", [])
        excluded_classes = classes.get("Exclude", [])

        if not included_classes and not excluded_classes:
            return True

        variables = dataset_metadata.variables if hasattr(dataset_metadata, "variables") else []
        domain_name = dataset_metadata.dataset_name
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

    # @staticmethod
    # def _ct_package_type_api_name(ct_package_type: str | None) -> str:
    #     if ct_package_type is None:
    #         return None
    #     return f"{ct_package_type.lower()}ct"

    def perform_rule_operations(
        self,
        rule: dict,
        current_domain: str,
        data_service: PostgresQLDataService,
    ) -> dict[str, SqlOperationResult]:
        """
        Each operation creates an output variable
        """
        operations: List[dict] = rule.get("operations") or []
        output_variables: dict[str, SqlOperationResult] = {}

        for operation in operations:
            rule_name = operation.get("operator", "")
            output_variable = operation.get("id", "")

            if not output_variable.startswith("$"):
                raise ValueError(
                    f"Output variable must start with '$', "
                    f"but got '{output_variable}' in rule {rule.get('core_id', 'unknown')}"
                )

            # change -- pattern to domain name
            target_variable: str = operation.get("name")
            operation_domain: str = operation.get("domain", current_domain)
            if target_variable and target_variable.startswith("--") and current_domain:
                # Not a study wide operation
                target_variable = target_variable.replace("--", current_domain)

                # TODO: WTF is this line doing?
                # domain = domain.replace("--", domain)

            # build parameters for the operation
            params = SqlOperationParams(
                domain=operation_domain,
                target=target_variable,
                standard=data_service.ig_specs.get("standard"),
                standard_version=data_service.ig_specs.get("standard_version"),
            )

            operation = SqlOperationsFactory.get_service(rule_name, params=params, data_service=data_service)
            query = operation.execute()
            output_variables[output_variable] = query

            logger.info(f"Processed rule operation. " f"operation={rule_name}, rule={rule}")
        return output_variables

    # def is_relationship_dataset(self, dataset_name: str) -> bool:
    #     # TODO: this should come from the library and from the dataset metadata
    #     if dataset_name in ["RELREC", "RELSUB", "CO"]:
    #         result = True
    #     elif dataset_name.startswith("SUPP"):
    #         result = True
    #     elif dataset_name.startswith("SQ"):
    #         result = True
    #     else:
    #         result = False
    #     # logger.info(f"is_relationship_dataset. dataset_name={dataset_name}, result={result}")
    #     return result

    # def get_size_unit_from_rule(self, rule: dict) -> Optional[str]:
    #     """
    #     Extracts size unit from rule if it was passed
    #     """
    #     rule_conditions: ConditionInterface = rule["conditions"]
    #     for condition in rule_conditions.values():
    #         value: dict = condition["value"]
    #         if value["target"] == "dataset_size":
    #             return value.get("unit")

    # def add_operator_to_rule_conditions(self, rule: dict, target_to_operator_map: dict, domain: str):
    #     """
    #     Adds "operator" key to rule condition.
    #     target_to_operator_map parameter is a dict
    #     where keys are targets and values are operators.

    #     The rule is passed and changed by reference.
    #     """
    #     conditions: ConditionInterface = rule["conditions"]
    #     for condition in conditions.values():
    #         target: str = condition.get("value", {}).get("target", "").replace("--", domain)
    #         operator_to_add: Optional[Union[str, list]] = target_to_operator_map.get(target)
    #         if not operator_to_add:
    #             continue
    #         if isinstance(operator_to_add, str):
    #             condition["operator"] = operator_to_add
    #         elif isinstance(operator_to_add, list):
    #             nested_conditions = [{**condition, "operator": operator} for operator in operator_to_add]
    #             condition.clear()  # delete all keys from dict
    #             condition[AllowedConditionsKeys.ANY.value] = nested_conditions

    # def add_comparator_to_rule_conditions(self, rule: dict, comparator: dict = None, target_prefix=None):
    #     """
    #     Adds "comparator" key to rule conditions.value key.

    #     comparator parameter is a dict where
    #     keys are targets and values are comparators.

    #     The rule is passed and changed by reference.
    #     """
    #     conditions: ConditionInterface = rule["conditions"]
    #     for condition in conditions.values():
    #         value: dict = condition["value"]
    #         if comparator:
    #             # Adding a specific value
    #             comparator_to_add = comparator.get(value["target"])
    #         elif target_prefix:
    #             # Referencing a target variable in another dataset
    #             comparator_to_add = f"{target_prefix}{value['target']}"
    #         else:
    #             comparator_to_add = None
    #         if comparator_to_add:
    #             value["comparator"] = comparator_to_add
    #     # logger.info(
    #         f"Added comparator to rule conditions. " f"comparator={comparator}, conditions={rule['conditions']}"
    #     )

    @staticmethod
    def duplicate_conditions_for_all_targets(conditions: ConditionInterface, targets: List[str]) -> dict:
        """
        Given a list of conditions duplicates the condition for all targets as necessary
        """
        conditions_dict = conditions.get_conditions()
        new_conditions_dict = {}
        for key, conditions_list in conditions_dict.items():
            new_conditions_list = []
            for condition in conditions_list:
                if condition.should_copy():
                    new_conditions_list.extend([condition.copy().set_target(target) for target in targets])
                else:
                    new_conditions_list.append(condition)
            new_conditions_dict[key] = new_conditions_list
        return new_conditions_dict

    def is_suitable_for_validation(
        self,
        rule: dict,
        dataset_metadata: SQLDatasetMetadata,
        standard,
        standard_substandard: str,
    ) -> Tuple[bool, str]:
        """Check if rule is suitable and return reason if not"""
        rule_id = rule.get("core_id", "unknown")
        dataset_name = dataset_metadata.dataset_name

        if not self.rule_applies_to_class(dataset_metadata, rule):
            reason = f"Rule skipped - doesn't apply to class for " f"rule id={rule_id}, dataset={dataset_name}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason
        if not self.rule_applies_to_domain(dataset_metadata, rule):
            reason = f"Rule skipped - doesn't apply to domain for rule id={rule_id}, dataset={dataset_name}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason

        logger.info(f"is_suitable_for_validation. rule id={rule_id}, dataset={dataset_name}, result=True")
        return True, ""

    # @staticmethod
    # def extract_referenced_variables_from_rule(rule: dict):
    #     """
    #     Extracts a list of all variables referenced in a rule.
    #     """
    #     target_names: List[str] = []
    #     conditions: ConditionInterface = rule["conditions"]
    #     for condition in conditions.values():
    #         target = condition["value"].get("target")
    #         comparator = condition["value"].get("comparator")
    #         if target:
    #             target_names.append(target)
    #         if comparator:
    #             target_names.append(comparator)
    #     return target_names

    # @staticmethod
    # def extract_message_from_rule(rule: dict) -> str:
    #     """
    #     Extracts message from rule.
    #     """
    #     actions: List[dict] = rule["actions"]
    #     return actions[0]["params"]["message"]

    @staticmethod
    def valid_rule_structure(rule) -> bool:
        required_keys = ["standards", "core_id"]
        for key in required_keys:
            if key not in rule:
                return False
        return True
