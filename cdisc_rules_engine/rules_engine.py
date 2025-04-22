from copy import deepcopy
from typing import Iterable, List, Union

from business_rules import export_rule_data
from business_rules.engine import run
import os
from cdisc_rules_engine.config import config as default_config
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DatasetNotFoundError,
    DomainNotFoundInDefineXMLError,
    RuleFormatError,
    VariableMetadataNotFoundError,
    FailedSchemaValidation,
    DomainNotFoundError,
)
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    ConfigInterface,
    DataServiceInterface,
)
from cdisc_rules_engine.models.actions import COREActions
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset_variable import DatasetVariable
from cdisc_rules_engine.models.failed_validation_entity import FailedValidationEntity
from cdisc_rules_engine.models.rule_conditions.condition_composite_factory import (
    ConditionCompositeFactory,
)
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.cache import CacheServiceFactory
from cdisc_rules_engine.services.data_services import DataServiceFactory
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.utilities.dataset_preprocessor import DatasetPreprocessor
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.utilities.utils import (
    serialize_rule,
)
from cdisc_rules_engine.dataset_builders import builder_factory
from cdisc_rules_engine.models.external_dictionaries_container import (
    ExternalDictionariesContainer,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
import traceback


class RulesEngine:
    def __init__(
        self,
        cache: CacheServiceInterface = None,
        data_service: DataServiceInterface = None,
        config_obj: ConfigInterface = None,
        external_dictionaries: ExternalDictionariesContainer = ExternalDictionariesContainer(),
        **kwargs,
    ):
        self.config = config_obj or default_config
        self.standard = kwargs.get("standard")
        self.standard_version = (kwargs.get("standard_version") or "").replace(".", "-")
        self.standard_substandard = kwargs.get("standard_substandard") or None
        self.library_metadata = kwargs.get("library_metadata")
        self.max_dataset_size = kwargs.get("max_dataset_size")
        self.dataset_paths = kwargs.get("dataset_paths")
        self.cache = cache or CacheServiceFactory(self.config).get_cache_service()
        data_service_factory = DataServiceFactory(
            config=self.config,
            cache_service=self.cache,
            standard=self.standard,
            standard_version=self.standard_version,
            standard_substandard=self.standard_substandard,
            library_metadata=self.library_metadata,
            max_dataset_size=self.max_dataset_size,
        )
        self.dataset_implementation = data_service_factory.get_dataset_implementation()
        kwargs["dataset_implementation"] = self.dataset_implementation
        self.data_service = data_service or data_service_factory.get_data_service(
            self.dataset_paths
        )
        self.rule_processor = RuleProcessor(
            self.data_service, self.cache, self.library_metadata
        )
        self.data_processor = DataProcessor(self.data_service, self.cache)
        self.ct_packages = kwargs.get("ct_packages", [])
        self.ct_package = kwargs.get("ct_package")
        self.external_dictionaries = external_dictionaries
        self.define_xml_path: str = kwargs.get("define_xml_path")
        self.validate_xml: bool = kwargs.get("validate_xml")

    def get_schema(self):
        return export_rule_data(DatasetVariable, COREActions)

    def validate_single_rule(self, rule: dict, datasets: Iterable[SDTMDatasetMetadata]):
        results = {}
        rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
            rule["conditions"]
        )
        for dataset_metadata in datasets:
            if dataset_metadata.unsplit_name in results and "domains" in rule:
                include_split = rule["domains"].get("include_split_datasets", False)
                if not include_split:
                    continue  # handling split datasets
            results[dataset_metadata.unsplit_name] = self.validate_single_dataset(
                rule,
                datasets,
                dataset_metadata,
            )
        return results

    def validate_single_dataset(
        self,
        rule: dict,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_metadata: SDTMDatasetMetadata,
    ) -> List[Union[dict, str]]:
        """
        This function is an entrypoint to validation process.
        It validates a given rule against datasets.
        """
        logger.info(
            f"Validating {dataset_metadata.name}. "
            f"rule={rule}. dataset_path={dataset_metadata.full_path}. datasets={datasets}."
        )
        try:
            is_suitable, reason = self.rule_processor.is_suitable_for_validation(
                rule,
                dataset_metadata,
                datasets,
                self.standard,
                self.standard_substandard,
            )
            if is_suitable:
                result: List[Union[dict, str]] = self.validate_rule(
                    rule, datasets, dataset_metadata
                )
                logger.info(
                    f"Validated dataset {dataset_metadata.name}. Result = {result}"
                )
                if result:
                    return result
                else:
                    # No errors were generated, create success error container
                    return [
                        ValidationErrorContainer(
                            **{
                                "dataset": dataset_metadata.filename,
                                "domain": dataset_metadata.domain
                                or dataset_metadata.rdomain,
                                "errors": [],
                            }
                        ).to_representation()
                    ]
            else:
                logger.info(
                    f"Skipped dataset {dataset_metadata.name}. Reason: {reason}"
                )
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    message=reason,
                    dataset=dataset_metadata.filename,
                    domain=dataset_metadata.domain or dataset_metadata.rdomain or "",
                )
                return [error_obj.to_representation()]
        except Exception as e:
            logger.trace(e)
            logger.error(
                f"""Error occurred during validation.
            Error: {e}
            Error Type: {type(e)}
            Error Message: {str(e)}
            Full traceback:
            {traceback.format_exc()}
            """
            )
            error_obj: ValidationErrorContainer = self.handle_validation_exceptions(
                e, dataset_metadata.full_path, dataset_metadata.full_path
            )
            error_obj.domain = dataset_metadata.domain or dataset_metadata.rdomain or ""
            # this wrapping into a list is necessary to keep return type consistent
            return [error_obj.to_representation()]

    def get_dataset_builder(
        self,
        rule: dict,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_metadata: SDTMDatasetMetadata,
    ):
        return builder_factory.get_service(
            rule.get("rule_type"),
            rule=rule,
            data_service=self.data_service,
            cache_service=self.cache,
            data_processor=self.data_processor,
            rule_processor=self.rule_processor,
            dataset_metadata=dataset_metadata,
            datasets=datasets,
            dataset_path=dataset_metadata.full_path,
            define_xml_path=self.define_xml_path,
            standard=self.standard,
            standard_version=self.standard_version,
            standard_substandard=self.standard_substandard,
            library_metadata=self.library_metadata,
            dataset_implementation=self.data_service.dataset_implementation,
        )

    def validate_rule(
        self,
        rule: dict,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_metadata: SDTMDatasetMetadata,
    ) -> List[Union[dict, str]]:
        """
         This function is an entrypoint for rule validation.
        It defines a rule validator based on its type and calls it.
        """
        kwargs = {}
        builder = self.get_dataset_builder(rule, datasets, dataset_metadata)
        dataset = builder.get_dataset()
        # Update rule for certain rule types
        # SPECIAL CASES FOR RULE TYPES ###############################
        # TODO: Handle these special cases better.
        if self.library_metadata:
            kwargs["variable_codelist_map"] = (
                self.library_metadata.variable_codelist_map
            )
            kwargs["codelist_term_maps"] = (
                self.library_metadata.get_all_ct_package_metadata()
            )
        if rule.get("rule_type") == RuleTypes.DEFINE_ITEM_METADATA_CHECK.value:
            if self.library_metadata:
                kwargs["variable_codelist_map"] = (
                    self.library_metadata.variable_codelist_map
                )
                kwargs["codelist_term_maps"] = (
                    self.library_metadata.get_all_ct_package_metadata()
                )
        elif (
            rule.get("rule_type")
            == RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value
            or rule.get("rule_type")
            == RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE_XML_AND_LIBRARY.value
        ):
            self.rule_processor.add_comparator_to_rule_conditions(
                rule, comparator=None, target_prefix="define_"
            )
        elif (
            rule.get("rule_type")
            == RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value
        ):
            value_level_metadata: List[dict] = self.get_define_xml_value_level_metadata(
                dataset_metadata.full_path, dataset_metadata.unsplit_name
            )
            kwargs["value_level_metadata"] = value_level_metadata

        elif (
            rule.get("rule_type")
            == RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value
        ):
            library_metadata: dict = self.library_metadata.variables_metadata.get(
                dataset_metadata.domain, {}
            )
            define_metadata: List[dict] = builder.get_define_xml_variables_metadata()
            targets: List[str] = (
                self.data_processor.filter_dataset_columns_by_metadata_and_rule(
                    dataset.columns.tolist(), define_metadata, library_metadata, rule
                )
            )
            rule_copy = deepcopy(rule)
            updated_conditions = RuleProcessor.duplicate_conditions_for_all_targets(
                rule_copy["conditions"], targets
            )
            rule_copy["conditions"].set_conditions(updated_conditions)
            # When duplicating conditions,
            # rule should be copied to prevent updates to concurrent rule executions
            return self.execute_rule(
                rule_copy, dataset, datasets, dataset_metadata, **kwargs
            )

        kwargs["ct_packages"] = list(self.ct_packages)

        logger.info(f"Using dataset build by: {builder.__class__}")
        return self.execute_rule(rule, dataset, datasets, dataset_metadata, **kwargs)

    def execute_rule(
        self,
        rule: dict,
        dataset: DatasetInterface,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_metadata: SDTMDatasetMetadata,
        value_level_metadata: List[dict] = None,
        variable_codelist_map: dict = None,
        codelist_term_maps: list = None,
        ct_packages: list = None,
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
        rule_copy = deepcopy(rule)
        updated_conditions = RuleProcessor.duplicate_conditions_for_all_targets(
            rule["conditions"], dataset.columns.to_list()
        )
        rule_copy["conditions"].set_conditions(updated_conditions)
        # Adding copy for now to avoid updating cached dataset
        dataset = deepcopy(dataset)
        # preprocess dataset
        dataset_preprocessor = DatasetPreprocessor(
            dataset, dataset_metadata, self.data_service, self.cache
        )
        dataset = dataset_preprocessor.preprocess(rule_copy, datasets)
        dataset = self.rule_processor.perform_rule_operations(
            rule_copy,
            dataset,
            dataset_metadata.unsplit_name,
            datasets,
            dataset_metadata.full_path,
            standard=self.standard,
            standard_version=self.standard_version,
            standard_substandard=self.standard_substandard,
            external_dictionaries=self.external_dictionaries,
            ct_packages=ct_packages,
        )
        relationship_data = {}
        if (
            dataset_metadata is not None
            and self.rule_processor.is_relationship_dataset(dataset_metadata.name)
        ):
            relationship_data = self.data_processor.preprocess_relationship_dataset(
                os.path.dirname(dataset_metadata.full_path), dataset, datasets
            )
        dataset_variable = DatasetVariable(
            dataset,
            column_prefix_map={"--": dataset_metadata.domain},
            relationship_data=relationship_data,
            value_level_metadata=value_level_metadata,
            column_codelist_map=variable_codelist_map,
            codelist_term_maps=codelist_term_maps,
        )
        results = []
        run(
            serialize_rule(rule_copy),  # engine expects a JSON serialized dict
            defined_variables=dataset_variable,
            defined_actions=COREActions(
                results,
                variable=dataset_variable,
                dataset_metadata=dataset_metadata,
                rule=rule,
                value_level_metadata=value_level_metadata,
            ),
        )
        return results

    def get_define_xml_value_level_metadata(
        self, dataset_path: str, domain_name: str
    ) -> List[dict]:
        """
        Gets Define XML variable metadata and returns it as dataframe.
        """
        define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
            dataset_path, self.define_xml_path, self.data_service, self.cache
        )
        return define_xml_reader.extract_value_level_metadata(domain_name=domain_name)

    def handle_validation_exceptions(  # noqa
        self, exception, dataset_path, file_name
    ) -> ValidationErrorContainer:
        if isinstance(exception, DatasetNotFoundError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="Dataset Not Found",
                message=exception.message,
            )
            message = "rule execution error"
        elif isinstance(exception, RuleFormatError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="Rule format error",
                message=exception.message,
            )
            message = "rule execution error"
        elif isinstance(exception, AssertionError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="Rule format error",
                message="Rule contains invalid operator",
            )
            message = "rule execution error"
        elif isinstance(exception, KeyError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="Column not found in data",
                message=exception.args[0],
            )
            message = "rule execution error"
        elif isinstance(exception, DomainNotFoundInDefineXMLError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error=DomainNotFoundInDefineXMLError.description,
                message=exception.args[0],
            )
            message = "rule execution error"
        elif isinstance(exception, VariableMetadataNotFoundError):
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error=VariableMetadataNotFoundError.description,
                message=exception.args[0],
            )
            message = "rule execution error"
        elif isinstance(exception, FailedSchemaValidation):
            if self.validate_xml:
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    error=FailedSchemaValidation.description,
                    message=exception.args[0],
                )
                message = "Schema Validation Error"
                errors = [error_obj]
                return ValidationErrorContainer(
                    errors=errors,
                    message=message,
                    status=ExecutionStatus.SUCCESS.value,
                    dataset=os.path.basename(dataset_path),
                )
            else:
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    dataset=os.path.basename(dataset_path),
                )
                message = "Skipped because schema validation is off"
                errors = [error_obj]
                return ValidationErrorContainer(
                    dataset=os.path.basename(dataset_path),
                    errors=errors,
                    message=message,
                    status=ExecutionStatus.SKIPPED.value,
                )
        elif isinstance(exception, DomainNotFoundError):
            error_obj = ValidationErrorContainer(
                dataset=os.path.basename(dataset_path),
                message=str(exception),
                status=ExecutionStatus.SKIPPED.value,
            )
            message = "rule evaluation skipped - operation domain not found"
            errors = [error_obj]
            return ValidationErrorContainer(
                dataset=os.path.basename(dataset_path),
                errors=errors,
                message=message,
                status=ExecutionStatus.SKIPPED.value,
            )
        else:
            error_obj = FailedValidationEntity(
                dataset=os.path.basename(dataset_path),
                error="An unknown exception has occurred",
                message=str(exception),
            )
            message = "rule execution error"
        errors = [error_obj]
        return ValidationErrorContainer(
            dataset=os.path.basename(dataset_path),
            errors=errors,
            message=message,
            status=ExecutionStatus.EXECUTION_ERROR.value,
        )
