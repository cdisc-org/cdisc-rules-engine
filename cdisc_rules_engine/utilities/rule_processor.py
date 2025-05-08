import re
from typing import Iterable, List, Optional, Set, Union, Tuple
from cdisc_rules_engine.interfaces.cache_service_interface import (
    CacheServiceInterface,
)
from cdisc_rules_engine.models.dataset.dataset_interface import (
    DatasetInterface,
)
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

import os
from cdisc_rules_engine.constants.classes import (
    FINDINGS_ABOUT,
    FINDINGS,
)
from cdisc_rules_engine.constants.domains import (
    AP_DOMAIN,
    APFA_DOMAIN,
    SUPPLEMENTARY_DOMAINS,
)
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.constants.use_cases import USE_CASE_DOMAINS
from cdisc_rules_engine.interfaces import ConditionInterface
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.rule_conditions import AllowedConditionsKeys
from cdisc_rules_engine.operations import operations_factory
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.utilities.utils import (
    get_directory_path,
    get_operations_cache_key,
    is_ap_domain,
    search_in_list_of_dicts,
    get_dataset_name_from_details,
)
from cdisc_rules_engine.models.external_dictionaries_container import (
    ExternalDictionariesContainer,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.interfaces.data_service_interface import (
    DataServiceInterface,
)
from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundError


class RuleProcessor:
    def __init__(
        self,
        data_service: DataServiceInterface,
        cache: CacheServiceInterface,
        library_metadata: LibraryMetadataContainer = None,
    ):
        self.data_service = data_service
        self.cache = cache
        self.library_metadata = library_metadata

    @classmethod
    def rule_applies_to_domain(
        cls, dataset_metadata: SDTMDatasetMetadata, rule: dict
    ) -> bool:
        """
        Check that rule is applicable to dataset domain
        """
        domains = rule.get("domains") or {}
        include_split_datasets: bool = domains.get("include_split_datasets")

        included_domains = domains.get("Include", [])
        excluded_domains = domains.get("Exclude", [])

        is_included = cls._is_domain_name_included(
            dataset_metadata, included_domains, include_split_datasets
        )
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
    def _is_domain_name_excluded(
        cls, dataset_metadata: SDTMDatasetMetadata, excluded_domains: List[str]
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
    def _domain_matched_ap_or_supp(
        cls, dataset_metadata: SDTMDatasetMetadata, domains_to_check: List[str]
    ) -> bool:
        """
        Check that domain name match with only
        AP / APFA / APRELSUB / SUPP / SQ naming pattern
        """
        supp_ap_domains = {f"{domain}--" for domain in SUPPLEMENTARY_DOMAINS}
        supp_ap_domains.update({f"{AP_DOMAIN}--", f"{APFA_DOMAIN}--"})

        return any(set(domains_to_check).intersection(supp_ap_domains)) and (
            dataset_metadata.is_supp
            or is_ap_domain(
                dataset_metadata.domain
                or dataset_metadata.rdomain
                or dataset_metadata.name
            )
        )

    def rule_applies_to_class(
        self,
        rule,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_metadata: SDTMDatasetMetadata,
    ):
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
        if included_classes:
            if ALL_KEYWORD in included_classes:
                return True
            variables = self.data_service.get_variables_metadata(
                dataset_name=dataset_metadata.full_path, datasets=datasets
            ).data.variable_name
            class_name = self.data_service.get_dataset_class(
                variables,
                dataset_metadata.full_path,
                datasets,
                dataset_metadata,
            )
            if (class_name not in included_classes) and not (
                class_name == FINDINGS_ABOUT and FINDINGS in included_classes
            ):
                is_included = False

        if excluded_classes:
            variables = self.data_service.get_variables_metadata(
                dataset_name=dataset_metadata.full_path, datasets=datasets
            ).data.variable_name
            class_name = self.data_service.get_dataset_class(
                variables,
                dataset_metadata.full_path,
                datasets,
                dataset_metadata,
            )
            if class_name and (
                (class_name in excluded_classes)
                or (class_name == FINDINGS_ABOUT and FINDINGS in excluded_classes)
            ):
                is_excluded = True
        return is_included and not is_excluded

    def rule_applies_to_use_case(
        self,
        dataset_metadata: SDTMDatasetMetadata,
        rule: dict,
        standard: str,
        standard_substandard: str,
    ) -> bool:
        if standard.lower() != "tig":
            return True
        use_cases = rule.get("use_case") or []
        if not use_cases:
            return True
        use_cases = [uc.strip() for uc in use_cases.split(",")]
        substandard = standard_substandard.upper()
        if substandard not in USE_CASE_DOMAINS:
            return False

        domain_to_check = dataset_metadata.domain
        if dataset_metadata.is_supp and dataset_metadata.rdomain:
            domain_to_check = dataset_metadata.rdomain

        # Handle ADaM datasets with AD prefix
        if substandard == "ADAM" and domain_to_check.startswith("AD"):
            return "ANALYSIS" in use_cases

        allowed_domains = set()
        for use_case in use_cases:
            if use_case in USE_CASE_DOMAINS[substandard]:
                allowed_domains.update(USE_CASE_DOMAINS[substandard][use_case])
        if domain_to_check in allowed_domains:
            return True
        return False

    def valid_rule_structure(self, rule) -> bool:
        required_keys = ["standards", "core_id"]
        for key in required_keys:
            if key not in rule:
                return False
        return True

    def perform_rule_operations(
        self,
        rule: dict,
        dataset: DatasetInterface,
        domain: str,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_path: str,
        standard: str,
        standard_version: str,
        standard_substandard: str,
        external_dictionaries: ExternalDictionariesContainer = ExternalDictionariesContainer(),
        **kwargs,
    ) -> DatasetInterface:
        """
        Applies rule operations to the dataset.
        Returns the processed dataset. Operation result is appended as a new column.
        """
        operations: List[dict] = rule.get("operations") or []
        if not operations:
            # stop function execution if no operations have been provided
            return dataset

        dataset_copy = dataset.copy()
        previous_operations = []
        for operation in operations:
            # change -- pattern to domain name
            original_target: str = operation.get("name")
            target: str = original_target
            domain: str = operation.get("domain", domain)
            if target and target.startswith("--") and domain:
                # Not a study wide operation
                target = target.replace("--", domain)
                domain = domain.replace("--", domain)

            # get necessary operation
            operation_params = OperationParams(
                core_id=rule.get("core_id"),
                operation_id=operation.get("id"),
                operation_name=operation.get("operator"),
                dataframe=dataset_copy,
                target=target,
                original_target=original_target,
                domain=domain,
                dataset_path=dataset_path,
                directory_path=get_directory_path(dataset_path),
                datasets=datasets,
                grouping=operation.get("group", []),
                standard=standard,
                standard_version=standard_version,
                standard_substandard=standard_substandard,
                external_dictionaries=external_dictionaries,
                ct_version=operation.get("version"),
                ct_attribute=operation.get("attribute"),
                ct_package_types=operation.get("ct_package_types"),
                ct_packages=kwargs.get("ct_packages"),
                ct_package=kwargs.get("codelist_term_maps"),
                attribute_name=operation.get("attribute_name", ""),
                key_name=operation.get("key_name", ""),
                key_value=operation.get("key_value", ""),
                case_sensitive=operation.get("case_sensitive", True),
                external_dictionary_type=operation.get("external_dictionary_type"),
                external_dictionary_term_variable=operation.get(
                    "external_dictionary_term_variable"
                ),
                dictionary_term_type=operation.get("dictionary_term_type"),
                filter=operation.get("filter", None),
                grouping_aliases=operation.get("group_aliases"),
                level=operation.get("level"),
                returntype=operation.get("returntype"),
                codelists=operation.get("codelists"),
                codelist=operation.get("codelist"),
            )

            # execute operation
            dataset_copy = self._execute_operation(
                operation_params, dataset_copy, previous_operations
            )
            previous_operations.append(operation_params.operation_name)

            logger.info(
                f"Processed rule operation. "
                f"operation={operation_params.operation_name}, rule={rule}"
            )
        return dataset_copy

    def _execute_operation(
        self,
        operation_params: OperationParams,
        dataset: DatasetInterface,
        previous_operations: List[str] = [],
    ):
        """
        Internal method that executes the given operation.
        Checks the cache first, if the operation result is not found
        in cache -> executes it and adds to the cache.
        """
        # check cache
        cache_key = get_operations_cache_key(
            core_id=operation_params.core_id,
            directory_path=operation_params.directory_path,
            operation_name=operation_params.operation_name,
            domain=operation_params.domain,
            grouping=";".join(operation_params.grouping),
            target_variable=operation_params.target,
            dataset_path=operation_params.dataset_path,
            operation_id=operation_params.operation_id,
        )
        if previous_operations:
            cache_key = f'{cache_key}-{";".join(previous_operations)}'
        result: DatasetInterface = self.cache.get(cache_key)
        if result is not None:
            return result

        if not self.is_current_domain(
            operation_params.dataframe, operation_params.domain
        ):
            # download other domain
            domain_details: dict = search_in_list_of_dicts(
                operation_params.datasets,
                lambda item: item.unsplit_name == operation_params.domain,
            )
            if domain_details is None:
                raise DomainNotFoundError(
                    f"Operation {operation_params.operation_name} requires Domain "
                    f"{operation_params.domain} but Domain not found in dataset"
                )
            filename = get_dataset_name_from_details(domain_details)
            file_path: str = os.path.join(
                get_directory_path(operation_params.dataset_path),
                filename,
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
            library_metadata=self.library_metadata,
        )
        result = operation.execute()
        if not DataProcessor.is_dummy_data(self.data_service):
            self.cache.add(cache_key, result)
        return result

    def is_current_domain(self, dataset, target_domain):
        if not target_domain:
            return True
        elif not self.is_relationship_dataset(target_domain):
            return "DOMAIN" in dataset and dataset["DOMAIN"].iloc[0] == target_domain
        else:
            # Always lookup relationship datasets when performing operations on them.
            return False

    def is_relationship_dataset(self, dataset_name: str) -> bool:
        # TODO: this should come from the library and from the dataset metadata
        if dataset_name in ["RELREC", "RELSUB", "CO"]:
            result = True
        elif dataset_name.startswith("SUPP"):
            result = True
        elif dataset_name.startswith("SQ"):
            result = True
        else:
            result = False
        logger.info(
            f"is_relationship_dataset. dataset_name={dataset_name}, result={result}"
        )
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
    def duplicate_conditions_for_all_targets(
        conditions: ConditionInterface, targets: List[str]
    ) -> dict:
        """
        Given a list of conditions duplicates the condition for all targets as necessary
        """
        conditions_dict = conditions.get_conditions()
        new_conditions_dict = {}
        for key, conditions_list in conditions_dict.items():
            new_conditions_list = []
            for condition in conditions_list:
                if condition.should_copy():
                    new_conditions_list.extend(
                        [condition.copy().set_target(target) for target in targets]
                    )
                else:
                    new_conditions_list.append(condition)
            new_conditions_dict[key] = new_conditions_list
        return new_conditions_dict

    def is_suitable_for_validation(
        self,
        rule: dict,
        dataset_metadata: SDTMDatasetMetadata,
        datasets: Iterable[SDTMDatasetMetadata],
        standard,
        standard_substandard: str,
    ) -> Tuple[bool, str]:
        """Check if rule is suitable and return reason if not"""
        rule_id = rule.get("core_id", "unknown")
        dataset_name = dataset_metadata.name
        if not self.valid_rule_structure(rule):
            reason = f"Rule skipped - invalid rule structure for rule id={rule_id}"
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason
        if not self.rule_applies_to_use_case(
            dataset_metadata, rule, standard, standard_substandard
        ):
            reason = (
                f"Rule skipped - doesn't apply to use case for "
                f"rule id={rule_id}, dataset={dataset_name}"
            )
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason
        if not self.rule_applies_to_domain(dataset_metadata, rule):
            reason = (
                f"Rule skipped - doesn't apply to domain for "
                f"rule id={rule_id}, dataset={dataset_name}"
            )
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason
        if not self.rule_applies_to_class(rule, datasets, dataset_metadata):
            reason = (
                f"Rule skipped - doesn't apply to class for "
                f"rule id={rule_id}, dataset={dataset_name}"
            )
            logger.info(f"is_suitable_for_validation. {reason}, result=False")
            return False, reason
        logger.info(
            f"is_suitable_for_validation. rule id={rule_id}, "
            f"dataset={dataset_name}, result=True"
        )
        return True, ""

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
                var.replace("--", domain or "", 1) for var in output_variables
            ]
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
