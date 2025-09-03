import re
from typing import List, Optional, Set, Tuple

from cdisc_rules_engine.config import config as default_config
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
    SQLDatasetMetadata,
)

# import os
# from cdisc_rules_engine.constants.classes import (
#     FINDINGS_ABOUT,
#     FINDINGS,
# )
# from cdisc_rules_engine.constants.domains import (
#     AP_DOMAIN,
#     APFA_DOMAIN,
#     SUPPLEMENTARY_DOMAINS,
# )
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD

# from cdisc_rules_engine.constants.use_cases import USE_CASE_DOMAINS
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
from cdisc_rules_engine.sql_operations import sql_operations_factory

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
        """Check that rule is applicable to dataset domain"""
        domains = rule.get("domains") or {}
        included_domains = domains.get("Include", [])
        excluded_domains = domains.get("Exclude", [])

        domain_name = dataset_metadata.dataset_name.upper()

        is_explicitly_included = domain_name in included_domains
        is_explicitly_excluded = domain_name in excluded_domains

        if is_explicitly_excluded:
            return False

        if is_explicitly_included:
            return True

        included_by_pattern = cls.matches_domain_pattern(domain_name, included_domains)
        excluded_by_pattern = cls.matches_domain_pattern(domain_name, excluded_domains)

        if included_domains and not included_by_pattern:
            return False

        if excluded_by_pattern:
            return False

        return True

    @staticmethod
    def matches_domain_pattern(domain: str, patterns: list) -> bool:
        """Check if domain matches any of the patterns."""
        for pattern in patterns:
            if pattern == ALL_KEYWORD:
                return True
            if pattern == domain:
                return True
            if pattern.endswith("--"):
                prefix = pattern[:-2]
                if domain.startswith(prefix):
                    return True
            if pattern.startswith("--"):
                suffix = pattern[2:]
                if domain.endswith(suffix):
                    return True
        return False

    # @classmethod
    # def _is_domain_name_included(
    #     cls,
    #     dataset_metadata: SDTMDatasetMetadata,
    #     included_domains: List[str],
    #     include_split_datasets: bool,
    # ) -> bool:
    #     """
    #     If included domains aren't specified
    #      and include_split_datasets is True,
    #      and it is not a split dataset
    #      -> domain is not included
    #     If included domains are specified,
    #      and the domain is not in the list of included domains,
    #      and domain doesn't match with AP / APFA / APRELSUB / SUPP / SQ naming pattern
    #      -> domain is not included.
    #     In other cases domain is included
    #     """
    #     if not included_domains:
    #         if include_split_datasets is True and not dataset_metadata.is_split:
    #             return False
    #         return True

    #     if (
    #         dataset_metadata.domain in included_domains
    #         or dataset_metadata.name in included_domains
    #         or ALL_KEYWORD in included_domains
    #     ):
    #         return True
    #     if cls._domain_matched_ap_or_supp(dataset_metadata, included_domains):
    #         return True
    #     return False

    # @classmethod
    # def _is_domain_name_excluded(cls, dataset_metadata: SDTMDatasetMetadata, excluded_domains: List[str]) -> bool:
    #     """
    #     If excluded domains are specified,
    #      and the domain is in the list of excluded domains,
    #      or domain name match with AP / APFA / APRELSUB / SUPP / SQ naming pattern
    #      domain is excluded.

    #     In other cases domain is not excluded.
    #     """
    #     if not excluded_domains:
    #         return False

    #     if (
    #         dataset_metadata.domain in excluded_domains
    #         or dataset_metadata.name in excluded_domains
    #         or dataset_metadata.unsplit_name in excluded_domains
    #         or ALL_KEYWORD in excluded_domains
    #     ):
    #         return True
    #     if cls._domain_matched_ap_or_supp(dataset_metadata, excluded_domains):
    #         return True
    #     return False

    # @classmethod
    # def _handle_split_domains(
    #     cls,
    #     is_split_domain: bool,
    #     include_split_datasets: bool,
    #     is_excluded: bool,
    #     is_included: bool,
    # ) -> Tuple[bool, bool]:
    #     """
    #     HANDLING SPLIT DOMAINS

    #     If include_split_datasets is True -
    #     add split domains to the list of included domains.
    #     If no included domains specified, only validate split domains

    #     If include_split_datasets is False - Exclude split domains
    #     If include_split_datasets is None - Do nothing
    #     """
    #     if include_split_datasets is True and is_split_domain and not is_excluded:
    #         is_included = True
    #     if include_split_datasets is False and is_split_domain:
    #         is_excluded = True
    #     return is_excluded, is_included

    # @classmethod
    # def _domain_matched_ap_or_supp(cls, dataset_metadata: SDTMDatasetMetadata, domains_to_check: List[str]) -> bool:
    #     """
    #     Check that domain name match with only
    #     AP / APFA / APRELSUB / SUPP / SQ naming pattern
    #     """
    #     supp_ap_domains = {f"{domain}--" for domain in SUPPLEMENTARY_DOMAINS}
    #     supp_ap_domains.update({f"{AP_DOMAIN}--", f"{APFA_DOMAIN}--"})

    #     return any(set(domains_to_check).intersection(supp_ap_domains)) and (
    #         dataset_metadata.is_supp
    #         or is_ap_domain(dataset_metadata.domain or dataset_metadata.rdomain or dataset_metadata.name)
    #     )

    # def rule_applies_to_class(
    #     self,
    #     rule,
    #     datasets: Iterable[SDTMDatasetMetadata],
    #     dataset_metadata: SDTMDatasetMetadata,
    # ):
    #     """
    #     If included classes are specified and the class
    #     is not in the list of included classes return false.

    #     If excluded classes are specified and the class
    #     is in the list of excluded classes return false

    #     Else return true.

    #     Rule authors can specify classes to include that we cannot detect.
    #     In this case, the get_dataset_class method will return None,
    #     but included_classes will have values.
    #     This will result in a rule not running when it is supposed to.
    #     We filter out non-detectable classes here, so that rule authors
    #     can specify them without it affecting if the rule runs or not.
    #     """
    #     classes = rule.get("classes") or {}
    #     included_classes = classes.get("Include", [])
    #     excluded_classes = classes.get("Exclude", [])
    #     is_included = True
    #     is_excluded = False
    #     if included_classes:
    #         if ALL_KEYWORD in included_classes:
    #             return True
    #         variables = self.data_service.get_variables_metadata(
    #             dataset_name=dataset_metadata.full_path, datasets=datasets
    #         ).data.variable_name
    #         class_name = self.data_service.get_dataset_class(
    #             variables,
    #             dataset_metadata.full_path,
    #             datasets,
    #             dataset_metadata,
    #         )
    #         if (class_name not in included_classes) and not (
    #             class_name == FINDINGS_ABOUT and FINDINGS in included_classes
    #         ):
    #             is_included = False

    #     if excluded_classes:
    #         variables = self.data_service.get_variables_metadata(
    #             dataset_name=dataset_metadata.full_path, datasets=datasets
    #         ).data.variable_name
    #         class_name = self.data_service.get_dataset_class(
    #             variables,
    #             dataset_metadata.full_path,
    #             datasets,
    #             dataset_metadata,
    #         )
    #         if class_name and (
    #             (class_name in excluded_classes) or (class_name == FINDINGS_ABOUT and FINDINGS in excluded_classes)
    #         ):
    #             is_excluded = True
    #     return is_included and not is_excluded

    # def rule_applies_to_use_case(
    #     self,
    #     dataset_metadata: SDTMDatasetMetadata,
    #     rule: dict,
    #     standard: str,
    #     standard_substandard: str,
    # ) -> bool:
    #     if standard.lower() != "tig":
    #         return True
    #     use_cases = rule.get("use_case") or []
    #     if not use_cases:
    #         return True
    #     use_cases = [uc.strip() for uc in use_cases.split(",")]
    #     substandard = standard_substandard.upper()
    #     if substandard not in USE_CASE_DOMAINS:
    #         return False

    #     domain_to_check = dataset_metadata.domain
    #     if dataset_metadata.is_supp and dataset_metadata.rdomain:
    #         domain_to_check = dataset_metadata.rdomain

    #     # Handle ADaM datasets with AD prefix
    #     if substandard == "ADAM" and domain_to_check.startswith("AD"):
    #         return "ANALYSIS" in use_cases

    #     allowed_domains = set()
    #     for use_case in use_cases:
    #         if use_case in USE_CASE_DOMAINS[substandard]:
    #             allowed_domains.update(USE_CASE_DOMAINS[substandard][use_case])
    #     if domain_to_check in allowed_domains:
    #         return True
    #     return False

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

            operation = sql_operations_factory.get_service(rule_name, params=params, data_service=data_service)
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

        if not self.rule_applies_to_domain(dataset_metadata, rule):
            reason = f"Rule skipped - doesn't apply to domain for rule id={rule_id}, dataset={dataset_name}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason
        # if not self.rule_applies_to_use_case(dataset_metadata, rule, standard, standard_substandard):
        #     reason = f"Rule skipped - doesn't apply to use case for " f"rule id={rule_id}, dataset={dataset_name}"
        #     logger.info(f"is_suitable_for_validation. {reason}, result=False")
        #     return False, reason
        # if not self.rule_applies_to_class(rule, datasets, dataset_metadata):
        #     reason = f"Rule skipped - doesn't apply to class for " f"rule id={rule_id}, dataset={dataset_name}"
        #     logger.info(f"is_suitable_for_validation. {reason}, result=False")
        #     return False, reason
        # TODO: uncomment and reimplement above other checks (i.e. class, use-case)

        logger.info(f"is_suitable_for_validation. rule id={rule_id}, dataset={dataset_name}, result=True")
        return True, ""

    @staticmethod
    def extract_target_names_from_rule(rule: dict, domain: str, column_names: List[str]) -> Set[str]:
        r"""
        Extracts target from each item of condition list.

        Some operators require reporting additional column names when
        extracting target names. An operator has a certain pattern,
        to which these column names have to correspond. So we
        have a mapping like {operator: pattern} to find the
        necessary pattern and extract matching column names.
        Example:
            column: TSVAL
            operator: additional_columns_empty
            pattern: ^TSVAL\d+$ (starts with TSVAL and ends with number)
            additional columns: TSVAL1, TSVAL2, TSVAL3 etc.
        """
        output_variables: List[str] = rule.get("output_variables", [])
        if output_variables:
            target_names: List[str] = [var.replace("--", domain or "", 1) for var in output_variables]
        else:
            target_names: List[str] = []
            conditions: ConditionInterface = rule["conditions"]
            for condition in conditions.values():
                if condition.get("operator") == "not_exists":
                    continue
                target: str = condition["value"].get("target")
                if target is None:
                    continue
                target = target.replace("--", domain or "")
                op_related_pattern: str = SQLRuleProcessor.get_operator_related_pattern(
                    condition.get("operator"), target
                )
                if op_related_pattern is not None:
                    target_names.extend(
                        filter(
                            lambda name: re.match(op_related_pattern, name),
                            column_names,
                        )
                    )
                else:
                    target_names.append(target)
        target_names.sort()
        return set([tn.lower() for tn in target_names])

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

    @staticmethod
    def get_operator_related_pattern(operator: str, target: str) -> Optional[str]:
        # {operator: pattern} mapping
        operator_related_patterns: dict = {
            "additional_columns_empty": rf"^{target}\d+$",
            "additional_columns_not_empty": rf"^{target}\d+$",
        }
        return operator_related_patterns.get(operator)

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
