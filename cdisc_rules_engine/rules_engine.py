from copy import deepcopy
from typing import List, Union

import pandas as pd
from business_rules import export_rule_data
from business_rules.engine import run

from cdisc_rules_engine.config import config as default_config
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DatasetNotFoundError,
    DomainNotFoundInDefineXMLError,
    RuleFormatError,
    VariableMetadataNotFoundError,
)
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    ConfigInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.models.actions import COREActions
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.models.failed_validation_entity import FailedValidationEntity
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.cache import CacheServiceFactory, InMemoryCacheService
from cdisc_rules_engine.services.data_services import DataServiceFactory
from cdisc_rules_engine.services.define_xml_reader import DefineXMLReader
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.utilities.dataset_preprocessor import DatasetPreprocessor
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.utilities.utils import (
    get_directory_path,
    get_library_variables_metadata_cache_key,
    get_standard_codelist_cache_key,
    get_standard_details_cache_key,
    is_split_dataset,
    serialize_rule,
)
from cdisc_rules_engine.dataset_builders import builder_factory


class RulesEngine:
    def __init__(
        self,
        cache: CacheServiceInterface = None,
        data_service: DataServiceInterface = None,
        config_obj: ConfigInterface = None,
        **kwargs,
    ):
        self.config = config_obj or default_config
        self.cache = cache or CacheServiceFactory(self.config).get_cache_service()
        self.data_service = (
            data_service or DataServiceFactory(self.config, self.cache).get_service()
        )
        self.rule_processor = RuleProcessor(self.data_service, self.cache)
        self.data_processor = DataProcessor(self.data_service, self.cache)
        self.standard = kwargs.get("standard")
        self.standard_version = kwargs.get("standard_version")
        self.ct_package = kwargs.get("ct_package")
        self.meddra_path: str = kwargs.get("meddra_path")
        self.whodrug_path: str = kwargs.get("whodrug_path")

    def get_schema(self):
        return export_rule_data(DatasetVariable, COREActions)

    def test_validation(
        self,
        rule: dict,
        dataset_path: str,
        datasets: List[DummyDataset],
        dataset_domain: str,
    ):
        self.data_service = DataServiceFactory(
            self.config, InMemoryCacheService.get_instance()
        ).get_dummy_data_service(datasets)
        dataset_dicts = []
        for domain in datasets:
            dataset_dicts.append({"domain": domain.domain, "filename": domain.filename})
        self.rule_processor = RuleProcessor(self.data_service, self.cache)
        self.data_processor = DataProcessor(self.data_service, self.cache)
        return self.validate_single_rule(
            rule, f"{dataset_path}", dataset_dicts, dataset_domain
        )

    def validate(
        self,
        rules: List[dict],
        dataset_path: str,
        datasets: List[dict],
        dataset_domain: str,
    ) -> dict:
        """
        This function is an entrypoint to validation process.
        It is a wrapper over validate_single_rule that allows
        to validate a list of rules.
        """
        logger.info(
            f"Validating domain {dataset_domain}. "
            f"dataset_path={dataset_path}. datasets={datasets}."
        )
        output = {}
        for rule in rules:
            result = self.validate_single_rule(
                rule, dataset_path, datasets, dataset_domain
            )
            # result may be None if a rule is not suitable for validation
            if result is not None:
                output[rule.get("core_id")] = result
        return output

    def validate_single_rule(
        self, rule: dict, dataset_path: str, datasets: List[dict], dataset_domain: str
    ) -> List[Union[dict, str]]:
        """
        This function is an entrypoint to validation process.
        It validates a given rule against datasets.
        """
        logger.info(
            f"Validating domain {dataset_domain}. "
            f"rule={rule}. dataset_path={dataset_path}. datasets={datasets}."
        )
        try:
            if self.rule_processor.is_suitable_for_validation(
                rule,
                dataset_domain,
                dataset_path,
                is_split_dataset(datasets, dataset_domain),
                datasets,
            ):
                result: List[Union[dict, str]] = self.validate_rule(
                    rule, dataset_path, datasets, dataset_domain
                )
                logger.info(f"Validated domain {dataset_domain}. Result = {result}")
                if result:
                    return result
                else:
                    # No errors were generated, create success error container
                    return [
                        ValidationErrorContainer(
                            **{"domain": dataset_domain, "errors": []}
                        ).to_representation()
                    ]
            else:
                logger.info(f"Skipped domain {dataset_domain}.")
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value
                )
                error_obj.domain = dataset_domain
                return [error_obj.to_representation()]
        except Exception as e:
            logger.error(
                f"Error occurred during validation. Error: {e}. Error message: {str(e)}"
            )
            error_obj: ValidationErrorContainer = self.handle_validation_exceptions(
                e, dataset_path, dataset_path
            )
            error_obj.domain = dataset_domain
            # this wrapping into a list is necessary to keep return type consistent
            return [error_obj.to_representation()]

    def get_dataset_builder(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str
    ):
        return builder_factory.get_service(
            rule.get("rule_type"),
            rule=rule,
            data_service=self.data_service,
            cache_service=self.cache,
            data_processor=self.data_processor,
            rule_processor=self.rule_processor,
            domain=domain,
            datasets=datasets,
            dataset_path=dataset_path,
        )

    def validate_rule(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str
    ) -> List[Union[dict, str]]:
        """
         This function is an entrypoint for rule validation.
        It defines a rule validator based on its type and calls it.
        """
        builder = self.get_dataset_builder(rule, dataset_path, datasets, domain)
        dataset = builder.get_dataset()
        kwargs = {}
        # Update rule for certain rule types
        # SPECIAL CASES FOR RULE TYPES ###############################
        # TODO: Handle these special cases better.
        if (
            rule.get("rule_type")
            == RuleTypes.DATASET_METADATA_CHECK_AGAINST_DEFINE.value
        ):
            # get Define XML metadata for domain and use it as a rule comparator
            define_metadata: dict = self.get_define_xml_metadata_for_domain(
                dataset_path, domain
            )
            self.rule_processor.add_comparator_to_rule_conditions(rule, define_metadata)

        elif rule.get("rule_type") == RuleTypes.DEFINE_ITEM_METADATA_CHECK.value:
            variable_codelist_map_key = get_standard_codelist_cache_key(
                self.standard, self.standard_version
            )
            variable_codelist_map = self.cache.get(variable_codelist_map_key) or {}
            codelist_term_maps = [
                self.cache.get(package) or {} for package in self.ct_package
            ]
            kwargs["variable_codelist_map"] = variable_codelist_map
            kwargs["codelist_term_maps"] = codelist_term_maps

        elif (
            rule.get("rule_type")
            == RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value
        ):
            self.rule_processor.add_comparator_to_rule_conditions(
                rule, comparator=None, target_prefix="define_"
            )

        elif (
            rule.get("rule_type")
            == RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value
        ):
            value_level_metadata: List[dict] = self.get_define_xml_value_level_metadata(
                dataset_path, domain
            )
            kwargs["value_level_metadata"] = value_level_metadata

        elif (
            rule.get("rule_type")
            == RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value
        ):
            library_metadata: dict = self.cache.get(
                get_library_variables_metadata_cache_key(
                    self.standard, self.standard_version
                )
            )
            define_metadata: List[dict] = builder.get_define_xml_variables_metadata()
            targets: List[
                str
            ] = self.data_processor.filter_dataset_columns_by_metadata_and_rule(
                dataset.columns.tolist(), define_metadata, library_metadata, rule
            )
            rule["conditions"] = RuleProcessor.duplicate_conditions_for_all_targets(
                rule["conditions"], targets
            )

        logger.info(f"Using dataset build by: {builder.__class__}")
        return self.execute_rule(
            rule, dataset, dataset_path, datasets, domain, **kwargs
        )

    def execute_rule(
        self,
        rule: dict,
        dataset: pd.DataFrame,
        dataset_path: str,
        datasets: List[dict],
        domain: str,
        value_level_metadata: List[dict] = None,
        variable_codelist_map: dict = None,
        codelist_term_maps: list = None,
    ) -> List[str]:
        """
        Executes the given rule on a given dataset.
        """
        if value_level_metadata is None:
            value_level_metadata = []
        if variable_codelist_map is None:
            variable_codelist_map = {}
        if codelist_term_maps is None:
            codelist_term_maps = []
        # Add conditions to rule for all variables if variables: all appears
        # in condition
        rule["conditions"] = RuleProcessor.duplicate_conditions_for_all_targets(
            rule["conditions"], dataset.columns.to_list()
        )
        # Adding copy for now to avoid updating cached dataset
        dataset = deepcopy(dataset)
        # preprocess dataset
        dataset_preprocessor = DatasetPreprocessor(
            dataset, domain, dataset_path, self.data_service, self.cache
        )
        dataset = dataset_preprocessor.preprocess(rule, datasets)
        dataset = self.rule_processor.perform_rule_operations(
            rule,
            dataset,
            domain,
            datasets,
            dataset_path,
            standard=self.standard,
            standard_version=self.standard_version,
            meddra_path=self.meddra_path,
            whodrug_path=self.whodrug_path,
        )
        relationship_data = {}
        if self.rule_processor.is_relationship_dataset(domain):
            relationship_data = self.data_processor.preprocess_relationship_dataset(
                dataset_path.rsplit("/", 1)[0], dataset, datasets
            )
        dataset_variable = DatasetVariable(
            dataset,
            column_prefix_map={"--": domain},
            relationship_data=relationship_data,
            value_level_metadata=value_level_metadata,
            column_codelist_map=variable_codelist_map,
            codelist_term_maps=codelist_term_maps,
        )
        results = []
        run(
            serialize_rule(rule),  # engine expects a JSON serialized dict
            defined_variables=dataset_variable,
            defined_actions=COREActions(
                results,
                variable=dataset_variable,
                domain=domain,
                rule=rule,
                value_level_metadata=value_level_metadata,
            ),
        )
        return results

    def get_define_xml_metadata_for_domain(
        self, dataset_path: str, domain_name: str
    ) -> dict:
        """
        Gets Define XML metadata and returns it as dict.
        """
        directory_path = get_directory_path(dataset_path)
        define_xml_path: str = f"{directory_path}/{DEFINE_XML_FILE_NAME}"
        define_xml_contents: bytes = self.data_service.get_define_xml_contents(
            dataset_name=define_xml_path
        )
        define_xml_reader = DefineXMLReader.from_file_contents(
            define_xml_contents, cache_service_obj=self.cache
        )
        return define_xml_reader.extract_domain_metadata(domain_name=domain_name)

    def is_custom_domain(self, domain: str) -> bool:
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """
        # request standard details from cache
        standard_details: dict = (
            self.cache.get(
                get_standard_details_cache_key(self.standard, self.standard_version)
            )
            or {}
        )

        # check if domain is in standard details
        return domain not in standard_details.get("domains", {})

    def get_define_xml_value_level_metadata(
        self, dataset_path: str, domain_name: str
    ) -> List[dict]:
        """
        Gets Define XML variable metadata and returns it as dataframe.
        """
        directory_path = get_directory_path(dataset_path)
        define_xml_path: str = f"{directory_path}/{DEFINE_XML_FILE_NAME}"
        define_xml_contents: bytes = self.data_service.get_define_xml_contents(
            dataset_name=define_xml_path
        )
        define_xml_reader = DefineXMLReader.from_file_contents(
            define_xml_contents, cache_service_obj=self.cache
        )
        return define_xml_reader.extract_value_level_metadata(domain_name=domain_name)

    def handle_validation_exceptions(
        self, exception, dataset_path, file_name
    ) -> ValidationErrorContainer:
        if isinstance(exception, DatasetNotFoundError):
            error_obj = FailedValidationEntity(
                error="Dataset Not Found", message=exception.message
            )
            message = "rule execution error"
        elif isinstance(exception, RuleFormatError):
            error_obj = FailedValidationEntity(
                error="Rule format error", message=exception.message
            )
            message = "rule execution error"
        elif isinstance(exception, AssertionError):
            error_obj = FailedValidationEntity(
                error="Rule format error", message="Rule contains invalid operator"
            )
            message = "rule execution error"
        elif isinstance(exception, KeyError):
            error_obj = FailedValidationEntity(
                error="Column not found in data", message=exception.args[0]
            )
            message = "rule execution error"
        elif isinstance(exception, DomainNotFoundInDefineXMLError):
            error_obj = FailedValidationEntity(
                error=DomainNotFoundInDefineXMLError.description,
                message=exception.args[0],
            )
            message = "rule execution error"
        elif isinstance(exception, VariableMetadataNotFoundError):
            error_obj = FailedValidationEntity(
                error=VariableMetadataNotFoundError.description,
                message=exception.args[0],
            )
            message = "rule execution error"
        else:
            error_obj = FailedValidationEntity(
                error="An unknown exception has occurred", message=str(exception)
            )
            message = "rule execution error"
        errors = [error_obj]
        return ValidationErrorContainer(
            errors=errors, message=message, status=ExecutionStatus.EXECUTION_ERROR.value
        )
