import copy
import re
from typing import List, Optional, Set, Union, Tuple

import pandas as pd

from cdisc_rules_engine.constants.classes import DETECTABLE_CLASSES
from cdisc_rules_engine.constants.domains import (
    AP_DOMAIN,
    APFA_DOMAIN,
    SUPPLEMENTARY_DOMAINS,
)
from cdisc_rules_engine.interfaces import ConditionInterface
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.rule_conditions import (
    AllowedConditionsKeys,
    ConditionCompositeFactory,
)
from cdisc_rules_engine.operations import operations_factory
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.utilities.utils import (
    get_directory_path,
    get_operations_cache_key,
    is_ap_domain,
    is_supp_domain,
    search_in_list_of_dicts,
)


class RuleProcessor:
    def __init__(self, data_service, cache):
        self.data_service = data_service
        self.cache = cache

    @classmethod
    def rule_applies_to_domain(
        cls, dataset_domain: str, rule: dict, is_split_domain: bool
    ) -> bool:
        """
        Check that rule is applicable to dataset domain
        """
        domains = rule.get("domains") or {}
        include_split_datasets: bool = domains.get("include_split_datasets")

        included_domains = domains.get("Include", [])
        excluded_domains = domains.get("Exclude", [])

        is_included = cls._is_domain_name_included(
            dataset_domain, included_domains, include_split_datasets, is_split_domain
        )
        is_excluded = cls._is_domain_name_excluded(dataset_domain, excluded_domains)

        # additional check for split domains based on the flag
        is_excluded, is_included = cls._handle_split_domains(
            is_split_domain, include_split_datasets, is_excluded, is_included
        )

        return is_included and not is_excluded

    @classmethod
    def _is_domain_name_included(
        cls,
        dataset_domain: str,
        included_domains: List[str],
        include_split_datasets: bool,
        is_split_domain: bool,
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
            if include_split_datasets is True and not is_split_domain:
                return False
            return True

        if dataset_domain in included_domains or "All" in included_domains:
            return True
        if cls._domain_matched_ap_or_supp(dataset_domain, included_domains):
            return True
        return False

    @classmethod
    def _is_domain_name_excluded(
        cls, dataset_domain: str, excluded_domains: List[str]
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

        if dataset_domain in excluded_domains or "All" in excluded_domains:
            return True
        if cls._domain_matched_ap_or_supp(dataset_domain, excluded_domains):
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
        cls, dataset_domain: str, domains_to_check: List[str]
    ) -> bool:
        """
        Check that domain name match with only
        AP / APFA / APRELSUB / SUPP / SQ naming pattern
        """
        supp_ap_domains = {f"{domain}--" for domain in SUPPLEMENTARY_DOMAINS}
        supp_ap_domains.update({f"{AP_DOMAIN}--", f"{APFA_DOMAIN}--"})

        return any(set(domains_to_check).intersection(supp_ap_domains)) and (
            is_supp_domain(dataset_domain) or is_ap_domain(dataset_domain)
        )

    def rule_applies_to_class(self, rule, file_path, datasets: List[dict]):
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
        standard_version: str,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Applies rule operations to the dataset.
        Returns the processed dataset. Operation result is appended as a new column.
        """
        operations: List[dict] = rule.get("operations") or []
        if not operations:
            # stop function execution if no operations have been provided
            return dataset

        dataset_copy = dataset.copy()
        for operation in operations:
            # change -- pattern to domain name
            target: str = operation.get("name")
            domain: str = operation.get("domain", domain)
            if target and target.startswith("--") and domain:
                # Not a study wide operation
                target = target.replace("--", domain)
                domain = domain.replace("--", domain)

            # get necessary operation
            operation_params = OperationParams(
                operation_id=operation.get("id"),
                operation_name=operation.get("operator"),
                dataframe=dataset_copy,
                target=target,
                domain=domain,
                dataset_path=dataset_path,
                directory_path=get_directory_path(dataset_path),
                datasets=datasets,
                grouping=operation.get("group", []),
                standard=standard,
                standard_version=standard_version,
                meddra_path=kwargs.get("meddra_path"),
                whodrug_path=kwargs.get("whodrug_path"),
            )

            # execute operation
            dataset_copy = self._execute_operation(operation_params, dataset_copy)

            logger.info(
                f"Processed rule operation. "
                f"operation={operation_params.operation_name}, rule={rule}"
            )

        return dataset_copy

    def _execute_operation(
        self, operation_params: OperationParams, dataset: pd.DataFrame
    ):
        """
        Internal method that executes the given operation.
        Checks the cache first, if the operation result is not found
        in cache -> executes it and adds to the cache.
        """
        # check cache
        cache_key = get_operations_cache_key(
            directory_path=operation_params.directory_path,
            operation_name=operation_params.operation_name,
            domain=operation_params.domain,
            grouping=";".join(operation_params.grouping),
            target_variable=operation_params.target,
        )
        result = self.cache.get(cache_key)
        if result:
            return result

        if not self.is_current_domain(
            operation_params.dataframe, operation_params.domain
        ):
            # download other domain
            domain_details: dict = search_in_list_of_dicts(
                operation_params.datasets,
                lambda item: item.get("domain") == operation_params.domain,
            )
            file_path: str = (
                f"{get_directory_path(operation_params.dataset_path)}/"
                f"{domain_details['filename']}"
            )
            operation_params.dataframe = self.data_service.get_dataset(
                dataset_name=file_path
            )

        # call the operation
        operation = operations_factory.get_service(
            operation_params.operation_name,
            operation_params=operation_params,
            original_dataset=dataset,
            cache=self.cache,
            data_service=self.data_service,
        )
        result = operation.execute()
        if not DataProcessor.is_dummy_data(self.data_service):
            self.cache.add(cache_key, result)
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
        target_to_operator_map parameter is a dict
        where keys are targets and values are operators.

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

        comparator parameter is a dict where
        keys are targets and values are comparators.

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
            f"Added comparator to rule conditions. "
            f"comparator={comparator}, conditions={rule['conditions']}"
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
            f"is_suitable_for_validation. rule id={rule.get('core_id')}, "
            f"domain={dataset_domain}, result={is_suitable}"
        )
        return is_suitable

    @staticmethod
    def extract_target_names_from_rule(
        rule: dict, domain: str, column_names: List[str]
    ) -> Set[str]:
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
