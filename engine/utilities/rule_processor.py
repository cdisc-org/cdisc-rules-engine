import copy
import re
from typing import Callable, List, Union, Set, Optional
import numpy as np

import pandas as pd

from engine.services import logger
from engine.utilities.utils import search_in_list_of_dicts
from engine.constants.classes import DETECTABLE_CLASSES
from engine.constants.domains import SUPP_DOMAIN, AP_DOMAIN, APFA_DOMAIN
from engine import cache_service_obj
from engine.models.rule_conditions import (
    ConditionCompositeFactory,
    ConditionInterface,
    AllowedConditionsKeys,
)
from engine.utilities.data_processor import DataProcessor
from engine.utilities.utils import is_supp_domain, get_operations_cache_key, is_ap_domain, get_directory_path


class RuleProcessor:
    def __init__(self, data_service):
        self.data_service = data_service

    def rule_applies_to_domain(
            self, dataset_domain: str, rule: dict, is_split_domain: bool
    ):
        """
        If included domains are specified, and the domain is not in the list of included classes return false.
        If excluded domain are specified, and the domain is in the list of excluded classes return false
        Else return true.

        ### HANDLING SPLIT DOMAINS ###
        If include_split_domains is True - add split domains to the list of included domains. If no included domains specified, only validate split domains
        If include_split_domains is False - Exclude split domains
        If include_split_domains is None - Do nothing
        """
        domains = rule.get("domains") or {}
        included_domains = domains.get("Include", [])
        excluded_domains = domains.get("Exclude", [])
        include_split_datasets: bool = domains.get("include_split_datasets")
        is_included = True
        is_excluded = False
        if included_domains:
            if dataset_domain not in included_domains and "All" not in included_domains:
                # check supp domains with SUPP-- naming pattern
                matches_supp_naming_pattern = any(
                    is_supp_domain(dataset_domain)
                    and included_domain == f"{SUPP_DOMAIN}--"
                    for included_domain in included_domains
                )
                # check domains with AP--/ APFA-- / APRELSUB naming pattern
                matches_ap_naming_pattern = any(
                    is_ap_domain(dataset_domain)
                    and included_domain in [f"{AP_DOMAIN}--", f"{APFA_DOMAIN}--"]
                    for included_domain in included_domains
                )
                if not (matches_supp_naming_pattern or matches_ap_naming_pattern):
                    is_included = False
        # Case where included domains are not specified and include_split_datasets == True
        # Only include split datasets
        elif include_split_datasets == True and not is_split_domain:
            is_included = False

        if excluded_domains:
            if dataset_domain in excluded_domains or "All" in excluded_domains:
                is_excluded = True
            # check supp domains with SUPP-- naming pattern
            matches_supp_naming_pattern = any(
                is_supp_domain(dataset_domain)
                and excluded_domain == f"{SUPP_DOMAIN}--"
                for excluded_domain in excluded_domains
            )
            # check domains with AP--/ APFA-- / APRELSUB naming pattern
            matches_ap_naming_pattern = any(
                is_ap_domain(dataset_domain)
                and excluded_domain in [f"{AP_DOMAIN}--", f"{APFA_DOMAIN}--"]
                for excluded_domain in excluded_domains
            )
            if matches_supp_naming_pattern or matches_ap_naming_pattern:
                is_excluded = True
        # additional check for split domains based on the flag
        if include_split_datasets is True and is_split_domain and not is_excluded:
            is_included = True
        if include_split_datasets is False and is_split_domain:
            is_excluded = True

        return is_included and not is_excluded

    def rule_applies_to_class(self, rule, file_path, datasets: List[dict]):
        """
        If included classes are specified, and the class is not in the list of included classes return false.
        If excluded classes are specified, and the class is in the list of excluded classes return false
        Else return true.
        """
        classes = rule.get("classes") or {}
        included_classes = classes.get("Include", [])
        # Rule authors can specify classes to include that we cannot detect.
        # In this case, the get_dataset_class method will return None, but included_classes will have values.
        # This will result in a rule not running when it is supposed to.
        # We filter out non-detectable classes here, so that rule authors can specify them without it affecting if the rule
        # runs or not.
        included_classes = [c for c in included_classes if c in DETECTABLE_CLASSES]
        excluded_classes = classes.get("Exclude", [])
        is_included = True
        is_excluded = False
        if included_classes:
            dataset = self.data_service.get_dataset(dataset_name=file_path)
            class_name = self.data_service.get_dataset_class(
                dataset, file_path, datasets
            )
            if class_name not in included_classes and "All" not in included_classes:
                is_included = False

        if excluded_classes:
            dataset = self.data_service.get_dataset(dataset_name=file_path)
            class_name = self.data_service.get_dataset_class(
                dataset, file_path, datasets
            )
            if class_name and class_name in excluded_classes:
                is_excluded = True
        return is_included and not is_excluded

    def valid_rule_structure(self, rule) -> bool:
        required_keys = ["standards", "core_id"]
        for key in required_keys:
            if key not in rule:
                return False
        return True

    def perform_rule_operations(
            self,
            rule: dict,
            dataset: pd.DataFrame,
            domain: str,
            datasets: List[dict],
            dataset_path: str,
            standard: str,
            standard_version: str) -> pd.DataFrame:
        domain_operator_map = {
            "min": DataProcessor.calc_min,
            "max": DataProcessor.calc_max,
            "mean": DataProcessor.calc_mean,
            "distinct": DataProcessor.get_unique_values,
            "min_date": DataProcessor.calc_min_date,
            "max_date": DataProcessor.calc_max_date,
            "dy": DataProcessor.calc_dy,
            "extract_metadata": DataProcessor.extract_metadata,
            "variable_exists": DataProcessor.variable_exists
        }
        study_operator_map = {
            "variable_value_count": DataProcessor.study_variable_value_occurrence_count,
            "variable_names": DataProcessor.get_variable_names_for_given_standard
        }
        dataset_copy = dataset.copy()
        directory_path = get_directory_path(dataset_path)
        for operation in rule.get("operations") or []:
            operator = operation.get("operator")
            target_domain = operation.get("domain", domain)
            target_variable = operation.get("name")
            value_id = operation.get("id")
            group_by = operation.get("group", [])
            if target_variable.startswith("--") and target_domain:
                # Not a study wide operation
                target_variable = target_variable.replace("--", domain)
                target_domain = target_domain.replace("--", domain)
            if operator in study_operator_map:
                # Perform study wide operation
                result = study_operator_map.get(operator)(
                    target_variable, datasets, directory_path, self.data_service, standard=standard,
                    standard_version=standard_version
                )
            else:
                # Domain specific operation
                cache_key = get_operations_cache_key(
                    directory_path=directory_path,
                    operation_name=operator,
                    domain=target_domain,
                    grouping=";".join(group_by),
                    target_variable=target_variable
                )
                operation = domain_operator_map.get(operator)
                result = cache_service_obj.get(cache_key)
                if result is None:
                    result = self.execute_operation(
                        operation,
                        datasets,
                        dataset_path,
                        dataset,
                        target_domain,
                        domain,
                        target_variable,
                        group_by
                    )

                if not DataProcessor.is_dummy_data(self.data_service):
                    cache_service_obj.add(cache_key, result)

            if group_by:
                # Handle grouped results
                result = result.rename(columns={target_variable: value_id})
                target_columns = group_by + [value_id]
                dataset_copy = dataset_copy.merge(
                    result[target_columns], on=group_by, how="left"
                )
            elif isinstance(result, np.ndarray):
                # Case where distinct operator was used.
                # add column with results, each value will be a set
                list_of_result_sets: List[set] = [set(result)] * len(dataset_copy)
                dataset_copy[value_id] = pd.Series(list_of_result_sets)
            elif isinstance(result, dict):
                list_of_results: List[dict] = [result] * len(dataset_copy)
                dataset_copy[value_id] = list_of_results
            else:
                # Handle single results
                dataset_copy[value_id] = result
            logger.info(f"Processed rule operation. operation={operation}, rule={rule}")

        return dataset_copy

    def execute_operation(
            self,
            operation: Callable,
            datasets: List[dict],
            dataset_path: str,
            dataset: pd.DataFrame,
            target_domain: str,
            domain: str,
            target_variable: str,
            group_by: List = None,
    ):
        if self.is_current_domain(dataset, target_domain):
            result = operation(
                dataset, target_variable, group_by, dataset_path=dataset_path
            )
        else:
            domain_details: dict = search_in_list_of_dicts(
                datasets, lambda item: item.get("domain") == target_domain
            )
            file_name = domain_details["filename"]
            file_path = get_directory_path(dataset_path) + f"/{file_name}"
            dataframe = self.data_service.get_dataset(dataset_name=file_path)
            result = operation(
                dataframe, target_variable, group_by, dataset_path=dataset_path
            )
        return result

    def is_current_domain(self, dataset, target_domain):
        if not self.is_relationship_dataset(target_domain):
            return "DOMAIN" in dataset and dataset["DOMAIN"][0] == target_domain
        else:
            # Always lookup relationship datasets when performing operations on them.
            return False

    def is_relationship_dataset(self, domain: str) -> bool:
        if domain in ["RELREC", "RELSUB", "CO"]:
            result = True
        elif domain.startswith("SUPP"):
            result = True
        else:
            result = False
        logger.info(f"is_relationship_dataset. domain={domain}, result={result}")
        return result

    def get_size_unit_from_rule(self, rule: dict) -> Optional[str]:
        """
        Extracts size unit from rule if it was passed
        """
        rule_conditions: ConditionInterface = rule["conditions"]
        for condition in rule_conditions.values():
            value: dict = condition["value"]
            if value["target"] == "dataset_size":
                return value.get("unit")

    def add_operator_to_rule_conditions(
            self, rule: dict, target_to_operator_map: dict, domain: str
    ):
        """
        Adds "operator" key to rule condition.
        target_to_operator_map parameter is a dict where keys are targets and values are operators.
        The rule is passed and changed by reference.
        """
        conditions: ConditionInterface = rule["conditions"]
        for condition in conditions.values():
            target: str = (
                condition.get("value", {}).get("target", "").replace("--", domain)
            )
            operator_to_add: Optional[Union[str, list]] = target_to_operator_map.get(
                target
            )
            if not operator_to_add:
                continue
            if isinstance(operator_to_add, str):
                condition["operator"] = operator_to_add
            elif isinstance(operator_to_add, list):
                nested_conditions = [
                    {**condition, "operator": operator} for operator in operator_to_add
                ]
                condition.clear()  # delete all keys from dict
                condition[AllowedConditionsKeys.ANY.value] = nested_conditions

    def add_comparator_to_rule_conditions(
            self, rule: dict, comparator: dict = None, target_prefix=None
    ):
        """
        Adds "comparator" key to rule conditions.value key.
        comparator parameter is a dict where keys are targets and values are comparators.
        The rule is passed and changed by reference.
        """
        conditions: ConditionInterface = rule["conditions"]
        for condition in conditions.values():
            value: dict = condition["value"]
            if comparator:
                # Adding a specific value
                comparator_to_add = comparator.get(value["target"])
            elif target_prefix:
                # Referencing a target variable in another dataset
                comparator_to_add = f"{target_prefix}{value['target']}"
            else:
                comparator_to_add = None
            if comparator_to_add:
                value["comparator"] = comparator_to_add
        logger.info(
            f"Added comparator to rule conditions. comparator={comparator}, conditions={rule['conditions']}"
        )

    @staticmethod
    def create_list_of_conditions_for_each_target(rule: dict, targets: List[str]):
        """
        Multiplies the rule conditions by the number of targets
        and adds a target to each condition.

        Input:
        rule = {
            "conditions": {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to"
                    }
                ]
            },
            ...
        }
        targets = ["AESTDY", "AESEQ", "DOMAIN"]

        Output:
        {
            "conditions": {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "AESTDY"},
                    },
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "AESEQ"},
                    },
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "DOMAIN"},
                    }
                ]
            },
            ...
        }
        The given rule is changed by reference.
        """
        conditions: ConditionInterface = rule["conditions"]
        new_conditions = {}
        for key, condition_list in conditions.items():
            key_conditions = RuleProcessor.create_list_of_target_conditions(
                condition_list, targets
            )
            new_conditions[key] = key_conditions
        rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
            new_conditions
        )

    @staticmethod
    def create_list_of_target_conditions(
            condition_list: List[dict], targets: List[str]
    ) -> List[dict]:
        """
        Accepts a list of conditions and targets.
        Returns a list of conditions X targets.
        See the example in create_list_of_conditions_for_each_target function.
        """
        result = []
        for condition in condition_list:
            target_conditions: List[dict] = []
            for target in targets:
                condition_copy: dict = copy.deepcopy(condition)
                value: dict = condition_copy.get("value", {})
                value["target"] = target
                condition_copy["value"] = value
                target_conditions.append(condition_copy)
            result.extend(target_conditions)
        return result

    def is_suitable_for_validation(
            self,
            rule: dict,
            dataset_domain: str,
            file_path: str,
            is_split_domain: bool,
            datasets: List[dict],
    ) -> bool:
        is_suitable: bool = (
                self.valid_rule_structure(rule)
                and self.rule_applies_to_domain(dataset_domain, rule, is_split_domain)
                and self.rule_applies_to_class(rule, file_path, datasets)
        )
        logger.info(
            f"is_suitable_for_validation. rule id={rule.get('core_id')}, domain={dataset_domain}, result={is_suitable}"
        )
        return is_suitable

    @staticmethod
    def extract_target_names_from_rule(
            rule: dict, domain: str, column_names: List[str]
    ) -> Set[str]:
        """
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
            target_names: List[str] = [
                var.replace("--", domain, 1) for var in output_variables
            ]
        else:
            target_names: List[str] = []
            conditions: ConditionInterface = rule["conditions"]
            for condition in conditions.values():
                target: str = condition["value"].get("target")
                if target is None:
                    continue
                target = target.replace("--", domain)
                op_related_pattern: str = RuleProcessor.get_operator_related_pattern(
                    condition.get("operator"), target
                )
                if op_related_pattern is not None:
                    # if pattern exists -> return only matching column names
                    target_names.extend(
                        filter(
                            lambda name: re.match(op_related_pattern, name),
                            column_names,
                        )
                    )
                else:
                    target_names.append(target)
        target_names.sort()
        return set(target_names)

    @staticmethod
    def extract_referenced_variables_from_rule(rule: dict):
        """
        Extracts a list of all variables referenced in a rule.
        """
        target_names: List[str] = []
        conditions: ConditionInterface = rule["conditions"]
        for condition in conditions.values():
            target = condition["value"].get("target")
            comparator = condition["value"].get("comparator")
            if target:
                target_names.append(target)
            if comparator:
                target_names.append(comparator)
        return target_names

    @staticmethod
    def get_operator_related_pattern(operator: str, target: str) -> Optional[str]:
        # {operator: pattern} mapping
        operator_related_patterns: dict = {
            "additional_columns_empty": rf"^{target}\d+$",
            "additional_columns_not_empty": rf"^{target}\d+$",
        }
        return operator_related_patterns.get(operator)

    @staticmethod
    def extract_message_from_rule(rule: dict) -> str:
        """
        Extracts message from rule.
        """
        actions: List[dict] = rule["actions"]
        return actions[0]["params"]["message"]
