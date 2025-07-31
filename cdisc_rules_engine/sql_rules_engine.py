from copy import deepcopy
from typing import List, Union

from business_rules import export_rule_data
from business_rules.engine import run
import os
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService, SQLDatasetMetadata
from cdisc_rules_engine.enums.execution_status import ExecutionStatus

# from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DatasetNotFoundError,
    DomainNotFoundInDefineXMLError,
    RuleFormatError,
    VariableMetadataNotFoundError,
    FailedSchemaValidation,
    DomainNotFoundError,
)
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.failed_validation_entity import FailedValidationEntity
from cdisc_rules_engine.models.rule_conditions.condition_composite_factory import (
    ConditionCompositeFactory,
)
from cdisc_rules_engine.models.sql_actions import SQLCOREActions
from cdisc_rules_engine.models.sql_variable import PostgresQLBusinessEngineObject
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor
from cdisc_rules_engine.utilities.utils import (
    serialize_rule,
)
import traceback


class SQLRulesEngine:
    def __init__(self, data_service: PostgresQLDataService):
        self.rule_processor = SQLRuleProcessor()
        self.data_service = data_service

    def get_schema(self):
        return export_rule_data(PostgresQLBusinessEngineObject, SQLCOREActions)

    def sql_validate_single_rule(self, rule: dict):
        results = {}
        rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])

        # iterate through all pre-processed user datasets
        for pp_ds_id in self.data_service.pre_processed_dfs.keys():
            dataset_metadata = self.data_service.get_dataset_metadata(pp_ds_id)

            is_suitable, reason = self.rule_processor.is_suitable_for_validation(
                rule,
                dataset_metadata,
                self.data_service.ig_specs.get("standard"),
                self.data_service.ig_specs.get("standard_substandard"),
            )
            if is_suitable:
                if dataset_metadata.unsplit_name in results and "domains" in rule:
                    include_split = rule["domains"].get("include_split_datasets", False)
                    if not include_split:
                        continue  # handling split datasets
                results[dataset_metadata.unsplit_name] = self.validate_single_dataset(rule, dataset_metadata)
            else:
                logger.info(f"Skipped dataset {dataset_metadata.dataset_name}. Reason: {reason}")
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    message=reason,
                    dataset=dataset_metadata.filename,
                    domain=dataset_metadata.domain or dataset_metadata.rdomain or "",
                )
                return [error_obj.to_representation()]
        return results

    def validate_single_dataset(
        self,
        rule: dict,
        dataset_metadata: SQLDatasetMetadata,
    ) -> List[Union[dict, str]]:
        """
        This function is an entrypoint to validation process.
        It validates a given rule against datasets.
        """
        logger.info(
            f"Validating {dataset_metadata.dataset_name}. "
            f"rule={rule}. dataset_path={dataset_metadata.filepath}. "
            f"datasets={self.data_service.get_uploaded_dataset_ids()}."
        )
        try:
            result: List[Union[dict, str]] = self.validate_rule(rule, dataset_metadata)
            logger.info(f"Validated dataset {dataset_metadata.dataset_name}. Result = {result}")
            if result:
                return result
            else:
                # No errors were generated, create success error container
                return [
                    ValidationErrorContainer(
                        **{
                            "dataset": dataset_metadata.filename,
                            "domain": dataset_metadata.domain or dataset_metadata.rdomain,
                            "errors": [],
                        }
                    ).to_representation()
                ]
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
                e, dataset_metadata.filepath, dataset_metadata.filepath
            )
            error_obj.domain = dataset_metadata.domain or dataset_metadata.rdomain or ""
            # this wrapping into a list is necessary to keep return type consistent
            return [error_obj.to_representation()]

    def validate_rule(
        self,
        rule: dict,
        dataset_metadata: SQLDatasetMetadata,
    ) -> List[Union[dict, str]]:
        """
         This function is an entrypoint for rule validation.
        It defines a rule validator based on its type and calls it.
        """
        # Update rule for certain rule types
        # SPECIAL CASES FOR RULE TYPES ###############################
        # TODO: Handle these special cases better.
        # if self.library_metadata:
        #     kwargs["variable_codelist_map"] = self.library_metadata.variable_codelist_map
        #     kwargs["codelist_term_maps"] = self.library_metadata.get_all_ct_package_metadata()
        # if rule.get("rule_type") == RuleTypes.DEFINE_ITEM_METADATA_CHECK.value:
        #     if self.library_metadata:
        #         kwargs["variable_codelist_map"] = self.library_metadata.variable_codelist_map
        #         kwargs["codelist_term_maps"] = self.library_metadata.get_all_ct_package_metadata()
        # elif (
        #     rule.get("rule_type") == RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value
        #     or rule.get("rule_type") == RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE_XML_AND_LIBRARY.value
        # ):
        #     self.rule_processor.add_comparator_to_rule_conditions(rule, comparator=None, target_prefix="define_")
        # elif rule.get("rule_type") == RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value:
        #     value_level_metadata: List[dict] = self.get_define_xml_value_level_metadata(
        #         dataset_metadata.full_path, dataset_metadata.unsplit_name
        #     )
        #     kwargs["value_level_metadata"] = value_level_metadata

        # elif rule.get("rule_type") == RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value:
        #     library_metadata: dict = self.library_metadata.variables_metadata.get(dataset_metadata.domain, {})
        #     define_metadata: List[dict] = builder.get_define_xml_variables_metadata()
        #     targets: List[str] = self.data_processor.filter_dataset_columns_by_metadata_and_rule(
        #         dataset.columns.tolist(), define_metadata, library_metadata, rule
        #     )
        #     rule_copy = deepcopy(rule)
        # updated_conditions = SQLRuleProcessor.duplicate_conditions_for_all_targets(rule_copy["conditions"], targets)
        #     rule_copy["conditions"].set_conditions(updated_conditions)
        #     # When duplicating conditions,
        #     # rule should be copied to prevent updates to concurrent rule executions
        #     return self.execute_rule(rule_copy, datasets, dataset_metadata, **kwargs)

        # logger.info(f"Using dataset build by: {builder.__class__}")
        return self.execute_rule(rule, dataset_metadata)

    def execute_rule(
        self,
        rule: dict,
        dataset_metadata: SQLDatasetMetadata,
        value_level_metadata: List[dict] = [],
        variable_codelist_map: dict = {},
        codelist_term_maps: list = [],
        ct_packages: list = None,
    ) -> List[str]:
        """
        Executes the given rule on a given dataset.
        """
        # Add conditions to rule for all variables if variables: all appears in condition
        rule_copy = deepcopy(rule)
        updated_conditions = SQLRuleProcessor.duplicate_conditions_for_all_targets(
            rule["conditions"],
            dataset_metadata.variables,
        )
        rule_copy["conditions"].set_conditions(updated_conditions)

        # PRE-PROCESSING -> move to ingest!!!!

        #   preprocess dataset
        #   dataset_preprocessor = SQLDatasetPreprocessor(dataset, dataset_metadata, self.data_service, self.cache)
        #   dataset = dataset_preprocessor.preprocess(rule_copy, datasets)

        #  OPERATIONS - these are actually rule-specific, so they belong here
        #  TODO: pass in dataservice
        processed_ds_id = self.rule_processor.perform_rule_operations(
            rule_copy,
            dataset_metadata.dataset_id,
            standard=self.data_service.ig_specs.get("standard"),
            standard_version=self.data_service.ig_specs.get("standard_version"),
            standard_substandard=self.data_service.ig_specs.get("standard_substandard"),
            ct_packages=ct_packages,
        )

        # VENMO ENGINE START - this is actually rule-specific, so it belongs here
        #  TODO: pass in dataservice
        validation_dataset = PostgresQLBusinessEngineObject(
            validation_dataset_id=processed_ds_id,
            sql_data_service=self.data_service,
            dataset=PandasDataset(self.data_service.data_dfs.get(processed_ds_id)),
            column_prefix_map={"--": dataset_metadata.domain},
            value_level_metadata=value_level_metadata,
            column_codelist_map=variable_codelist_map,
            codelist_term_maps=codelist_term_maps,
        )
        results = []
        run(
            serialize_rule(rule_copy),  # engine expects a JSON serialized dict
            defined_variables=validation_dataset,
            defined_actions=SQLCOREActions(
                results, validation_dataset=validation_dataset, dataset_metadata=dataset_metadata, rule=rule
            ),
        )
        return results

    # def get_define_xml_value_level_metadata(self, dataset_path: str, domain_name: str) -> List[dict]:
    #     """
    #     Gets Define XML variable metadata and returns it as dataframe.
    #     """
    #     define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
    #         dataset_path, self.define_xml_path, self.data_service, self.cache
    #     )
    #     return define_xml_reader.extract_value_level_metadata(domain_name=domain_name)

    def handle_validation_exceptions(self, exception, dataset_path, file_name) -> ValidationErrorContainer:  # noqa
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
