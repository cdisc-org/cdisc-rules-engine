from copy import deepcopy
from typing import Callable, List, Union

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
from cdisc_rules_engine.models.dataset_types import DatasetTypes
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
    get_corresponding_datasets,
    get_directory_path,
    get_library_variables_metadata_cache_key,
    get_standard_codelist_cache_key,
    get_standard_details_cache_key,
    is_split_dataset,
    serialize_rule,
)


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
            rule, f"/{dataset_path}", dataset_dicts, dataset_domain
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

    def validate_rule(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str
    ) -> List[Union[dict, str]]:
        """
        This function is an entrypoint for rule validation.
        It defines a rule validator based on its type and calls it.
        """
        validator: Callable = self.get_dataset_validator(rule.get("rule_type"))
        logger.info(f"Validator function name: {validator.__name__}")
        return validator(rule, dataset_path, datasets, domain)

    def get_dataset_validator(self, rule_type: str) -> Callable:
        """
        Returns a validator function based on rule type.
        When extending the function bear in mind that validator
        functions should have the same interface or kwargs specified.
        """
        rule_type_validator_map: dict = {
            RuleTypes.DATASET_METADATA_CHECK.value: self.validate_dataset_metadata,
            RuleTypes.DATASET_METADATA_CHECK_AGAINST_DEFINE.value: (
                self.validate_dataset_metadata_against_define_xml
            ),
            RuleTypes.VARIABLE_METADATA_CHECK.value: self.validate_variables_metadata,
            RuleTypes.DOMAIN_PRESENCE_CHECK.value: self.validate_domain_presence,
            RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value: (
                self.validate_variable_metadata_against_define_xml
            ),
            RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value: (
                self.validate_value_level_metadata_against_define_xml
            ),
            RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value: (
                self.validate_dataset_contents_against_define_and_library
            ),
            RuleTypes.DEFINE.value: self.validate_define_xml,
        }
        return rule_type_validator_map.get(
            rule_type,
            self.validate_dataset_contents,  # by default validate dataset contents
        )

    def validate_dataset_contents(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str
    ) -> List[Union[dict, str]]:
        """
        Validates dataset contents.
        """
        dataset: pd.DataFrame = self.get_dataset_to_validate(
            DatasetTypes.CONTENTS.value, dataset_path, datasets, domain
        )
        return self.execute_rule(rule, dataset, dataset_path, datasets, domain)

    def validate_dataset_metadata(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str
    ) -> List[Union[dict, str]]:
        """
        Validates metadata of a given dataset.
        Checks file name, size, label etc.
        """
        size_unit: str = self.rule_processor.get_size_unit_from_rule(rule)
        dataset: pd.DataFrame = self.get_dataset_to_validate(
            DatasetTypes.METADATA.value,
            dataset_path,
            datasets,
            domain,
            size_unit=size_unit,
        )
        return self.execute_rule(rule, dataset, dataset_path, datasets, domain)

    def validate_dataset_metadata_against_define_xml(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str
    ) -> List[Union[dict, str]]:
        """
        Validates dataset metadata against define xml.
        The idea here is to get define XML metadata for a domain
        and use it as comparator in rule.
        """
        # get Define XML metadata for domain and use it as a rule comparator
        define_metadata: dict = self.get_define_xml_metadata_for_domain(
            dataset_path, domain
        )
        self.rule_processor.add_comparator_to_rule_conditions(rule, define_metadata)

        # get dataset metadata and execute the rule
        dataset: pd.DataFrame = self.get_dataset_to_validate(
            DatasetTypes.METADATA.value, dataset_path, datasets, domain
        )
        return self.execute_rule(rule, dataset, dataset_path, datasets, domain)

    def validate_variable_metadata_against_define_xml(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str
    ) -> List[Union[dict, str]]:
        """
        Validates variable metadata against define xml.
        The idea here is to get define XML metadata for all variables, and create
        a dataframe by joining variable metadata from xpt with define metadata.
        the comparator
        """
        # get Define XML metadata for domain and use it as a rule comparator
        variable_metadata: List[dict] = self.get_define_xml_variables_metadata(
            dataset_path, domain
        )
        self.rule_processor.add_comparator_to_rule_conditions(
            rule, comparator=None, target_prefix="define_"
        )
        # get dataset metadata and execute the rule
        xpt_metadata: pd.DataFrame = self.get_dataset_to_validate(
            DatasetTypes.VARIABLES_METADATA.value,
            dataset_path,
            datasets,
            domain,
            drop_duplicates=True,
        )
        define_metadata: pd.DataFrame = pd.DataFrame(variable_metadata)
        dataset = xpt_metadata.merge(
            define_metadata,
            left_on="variable_name",
            right_on="define_variable_name",
            how="left",
        )
        return self.execute_rule(rule, dataset, dataset_path, datasets, domain)

    def validate_define_xml(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str
    ) -> List[Union[dict, str]]:

        # get Define XML metadata for domain and use it as a rule comparator
        variable_metadata: List[dict] = self.get_define_xml_variables_metadata(
            dataset_path, domain
        )
        define_metadata: pd.DataFrame = pd.DataFrame(variable_metadata)
        variable_codelist_map_key = get_standard_codelist_cache_key(
            self.standard, self.standard_version
        )
        variable_codelist_map = self.cache.get(variable_codelist_map_key) or {}
        codelist_term_maps = [
            self.cache.get(package) or {} for package in self.ct_package
        ]
        return self.execute_rule(
            rule,
            define_metadata,
            dataset_path,
            datasets,
            domain,
            variable_codelist_map=variable_codelist_map,
            codelist_term_maps=codelist_term_maps,
        )

    def validate_value_level_metadata_against_define_xml(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str, **kwargs
    ) -> List[Union[dict, str]]:
        """
        Validates variable metadata against define xml.
        The idea here is to get define XML metadata for all variables, and create
        a dataframe by joining variable metadata from xpt with define metadata.
        the comparator
        """
        # get Define XML metadata for domain and use it as a rule comparator
        value_level_metadata: List[dict] = self.get_define_xml_value_level_metadata(
            dataset_path, domain
        )
        dataset: pd.DataFrame = self.get_dataset_to_validate(
            DatasetTypes.CONTENTS.value, dataset_path, datasets, domain
        )
        return self.execute_rule(
            rule, dataset, dataset_path, datasets, domain, value_level_metadata
        )

    def validate_dataset_contents_against_define_and_library(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str, **kwargs
    ) -> List[Union[dict, str]]:
        """
        Validates dataset contents against Define.XML variable metadata
        and Library variable metadata.

        Example rule:
            Library Variable Core Status = Permissible AND
            Define.xml Variable Origin Type = Collected AND
            Variable value is null
        To validate the example, we need:
            1. Extract variable metadata from CDISC Library;
            2. Extract variable metadata from Define.XML;
            3. Extract variable value from dataset.
        """
        # get metadata from library and define
        library_metadata: dict = self.cache.get(
            get_library_variables_metadata_cache_key(
                self.standard, self.standard_version
            )
        )
        define_metadata: List[dict] = self.get_define_xml_variables_metadata(
            dataset_path, domain
        )

        # create a list of targets based on dataset columns and use them in rule
        dataset: pd.DataFrame = self.get_dataset_to_validate(
            DatasetTypes.CONTENTS.value, dataset_path, datasets, domain
        )
        targets: List[
            str
        ] = self.data_processor.filter_dataset_columns_by_metadata_and_rule(
            dataset.columns.tolist(), define_metadata, library_metadata, rule
        )
        rule["conditions"] = RuleProcessor.duplicate_conditions_for_all_targets(
            rule["conditions"], targets
        )
        # execute the rule
        return self.execute_rule(rule, dataset, dataset_path, datasets, domain)

    def validate_variables_metadata(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str, **kwargs
    ) -> List[Union[dict, str]]:
        """
        Validates metadata of a single variable.
        """
        dataset: pd.DataFrame = self.get_dataset_to_validate(
            DatasetTypes.VARIABLES_METADATA.value,
            dataset_path,
            datasets,
            domain,
            drop_duplicates=True,
        )
        return self.execute_rule(rule, dataset, dataset_path, datasets, domain)

    def validate_domain_presence(
        self, rule: dict, dataset_path: str, datasets: List[dict], domain: str, **kwargs
    ) -> List[Union[dict, str]]:
        """
        Validates dataset presence against the
        datasets provided in the request.

        dataset example:
           AE      EC
        0  ae.xpt  ec.xpt
        """
        dataset: pd.DataFrame = pd.DataFrame(
            {ds["domain"]: ds["filename"] for ds in datasets}, index=[0]
        )
        logger.info(f"Validating domain presence. domain={domain}")
        return self.execute_rule(rule, dataset, dataset_path, datasets, domain)

    def get_dataset_to_validate(
        self,
        dataset_type: str,
        dataset_path: str,
        datasets: List[dict],
        domain: str,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Gets the necessary dataset from the storage.
        The idea is to return the needed dataset based on
        what we need to validate: contents, metadata or variables metadata
        AND handle the case when the dataset is split into multiple files.

        dataset_type param can be:
        dataset_contents, dataset_metadata or variables_metadata.
        For passing additional params to storage calls, kwargs can be used.
        """
        dataset_type_function_map: dict = {
            DatasetTypes.CONTENTS.value: self.data_service.get_dataset,
            DatasetTypes.METADATA.value: self.data_service.get_dataset_metadata,
            DatasetTypes.VARIABLES_METADATA.value: (
                self.data_service.get_variables_metadata
            ),
        }
        func_to_call: Callable = dataset_type_function_map[dataset_type]
        if not is_split_dataset(datasets, domain):
            # single dataset. the most common case
            dataset: pd.DataFrame = func_to_call(dataset_name=dataset_path, **kwargs)
        else:
            dataset: pd.DataFrame = self.data_service.join_split_datasets(
                func_to_call=func_to_call,
                dataset_names=self.get_corresponding_datasets_names(
                    dataset_path, datasets, domain
                ),
                **kwargs,
            )
        return dataset

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

    def get_define_xml_variables_metadata(
        self, dataset_path: str, domain_name: str
    ) -> List[dict]:
        """
        Gets Define XML variables metadata.
        """
        directory_path = get_directory_path(dataset_path)
        define_xml_path: str = f"{directory_path}/{DEFINE_XML_FILE_NAME}"
        define_xml_contents: bytes = self.data_service.get_define_xml_contents(
            dataset_name=define_xml_path
        )
        define_xml_reader = DefineXMLReader.from_file_contents(
            define_xml_contents, cache_service_obj=self.cache
        )
        return define_xml_reader.extract_variables_metadata(domain_name=domain_name)

    def get_corresponding_datasets_names(
        self, dataset_path: str, datasets: List[dict], domain: str
    ) -> List[str]:
        directory_path = get_directory_path(dataset_path)
        return [
            f"{directory_path}/{dataset['filename']}"
            for dataset in get_corresponding_datasets(datasets, domain)
        ]

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
