import re
import traceback
from copy import deepcopy
from typing import Callable, List, Union

from business_rules import export_rule_data
from business_rules.engine import check_conditions_recursively, run
from psycopg2 import Error as PostgresError
from psycopg2.errors import ProgrammingError

from cdisc_rules_engine.check_operators.sql.sql_truth import CompileUnsupported, SqlTruth
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.sensitivity import Sensitivity

# from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DatasetNotFoundError,
    DomainNotFoundError,
    DomainNotFoundInDefineXMLError,
    FailedSchemaValidation,
    RuleFormatError,
    VariableMetadataNotFoundError,
    ColumnNotFoundError,
    SqlOperatorError,
)
from cdisc_rules_engine.models.failed_validation_entity import FailedValidationEntity
from cdisc_rules_engine.models.rule_conditions.condition_composite_factory import (
    ConditionCompositeFactory,
)
from cdisc_rules_engine.models.sql_venmo_object import SqlVenmoObject
from cdisc_rules_engine.models.sql_venmo_result_handler import (
    SqlVenmoResultHandler,
)
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.sql_dataset_builders import sql_builder_factory
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlOperationError
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.utilities.rule_progress import (
    DisabledRuleProgressReporter,
    RuleProgressPhase,
    RuleProgressReporter,
)
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor
from cdisc_rules_engine.utilities.utils import (
    serialize_rule,
)


def clean_postgres_message(message: str) -> str:
    """Clean non-deterministic details from PostgreSQL error messages."""
    if "does not exist" in message:
        return "column or relation does not exist"
    elif "syntax error" in message:
        return "SQL syntax error"
    elif "invalid input syntax" in message:
        return "invalid input syntax"
    return message


class SQLRulesEngine:
    def __init__(
        self,
        data_service: PostgresQLDataService,
        standards_context: BaseStandardsContext,
        progress_reporter: RuleProgressReporter = None,
        compile_rules: bool = True,
    ):
        self.data_service = data_service
        self.standards_context = standards_context
        self.compile_rules = compile_rules
        self.progress_reporter = progress_reporter or DisabledRuleProgressReporter()

    def get_schema(self):
        return export_rule_data(SqlVenmoObject, SqlVenmoResultHandler)

    def sql_validate_single_rule(self, rule: dict):
        results = {}
        self.progress_reporter.set_phase(RuleProgressPhase.PARSING)
        rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
        is_study_sensitivity = rule.get("sensitivity") == Sensitivity.STUDY.value
        study_error_already_reported = False

        # Collect all dataset metadata for builders that need it (e.g., DomainListDatasetBuilder)
        all_datasets = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]

        # iterate through all pre-processed user datasets
        for pp_ds_id in self.data_service.get_uploaded_dataset_ids():
            dataset_metadata = self.data_service.get_dataset_metadata(pp_ds_id)

            is_suitable, reason = self.standards_context.within_rule_scope(
                rule,
                next(
                    (metadata for metadata in self.data_service.datasets if metadata.name.lower() == pp_ds_id.lower()),
                    None,
                ),
            )
            if is_suitable:
                if is_study_sensitivity and study_error_already_reported:
                    results[dataset_metadata.name] = self._containers_per_reported_file(
                        dataset_metadata,
                        lambda filename: ValidationErrorContainer(
                            **{
                                "dataset": filename,
                                "domain": dataset_metadata.domain,
                                "errors": [],
                            }
                        ),
                    )
                    continue

                dataset_results = self.validate_single_dataset(rule, dataset_metadata, all_datasets)
                results[dataset_metadata.name] = dataset_results
                if is_study_sensitivity and self._contains_error_entries(dataset_results):
                    study_error_already_reported = True
            else:
                logger.info(f"Skipped dataset {dataset_metadata.name}. Reason: {reason}")
                results[pp_ds_id] = self._containers_per_reported_file(
                    dataset_metadata,
                    lambda filename: ValidationErrorContainer(
                        status=ExecutionStatus.SKIPPED.value,
                        message=reason,
                        dataset=filename,
                        domain=dataset_metadata.domain,
                    ),
                )
        return results

    @staticmethod
    def _containers_per_reported_file(
        dataset_metadata: BaseDatasetMetadata,
        build_container: Callable[[str], ValidationErrorContainer],
    ) -> List[dict]:
        """Build one result container per file the dataset is reported under."""
        filenames = getattr(dataset_metadata, "split_part_filenames", None) or [dataset_metadata.filename]
        return [build_container(filename).to_representation() for filename in sorted(filenames)]

    @staticmethod
    def _contains_error_entries(result_entries: List[Union[dict, str]]) -> bool:
        """
        Returns True when a dataset result includes at least one reported validation error.
        """
        for entry in result_entries:
            if isinstance(entry, dict) and entry.get("errors"):
                return True
        return False

    def validate_single_dataset(
        self,
        rule: dict,
        dataset_metadata: BaseDatasetMetadata,
        datasets: List[BaseDatasetMetadata],
    ) -> List[Union[dict, str]]:
        """
        This function is an entrypoint to validation process.
        It validates a given rule against datasets.
        """
        logger.info(f"Validating {dataset_metadata.name}. rule={rule}.")
        try:
            result: List[Union[dict, str]] = self.validate_rule(rule, dataset_metadata, datasets)
            logger.info(f"Validated dataset {dataset_metadata.name}. Result = {result}")
            if result:
                return result
            else:
                # No errors were generated, create success error container
                return self._containers_per_reported_file(
                    dataset_metadata,
                    lambda filename: ValidationErrorContainer(
                        **{
                            "dataset": filename,
                            "domain": dataset_metadata.domain,
                            "errors": [],
                        }
                    ),
                )
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
            error_obj: ValidationErrorContainer = self.handle_validation_exceptions(e, dataset_metadata.filename)
            error_obj.domain = dataset_metadata.domain
            # this wrapping into a list is necessary to keep return type consistent
            return [error_obj.to_representation()]

    def validate_rule(
        self,
        rule: dict,
        dataset_metadata: BaseDatasetMetadata,
        datasets: List[BaseDatasetMetadata],
    ) -> List[Union[dict, str]]:
        """
        This function is an entrypoint for rule validation.
        It uses the sql_builder_factory to get the correct data source
        and then executes the rule against it.
        """
        self.progress_reporter.set_phase(RuleProgressPhase.EXECUTING)
        builder = sql_builder_factory.get_service(
            rule_type=rule.get("rule_type"),
            rule=rule,
            data_service=self.data_service,
            dataset_metadata=dataset_metadata,
            datasets=datasets,
            standards_context=self.standards_context,
        )

        dataset_id = builder.get_dataset_id()

        return self.execute_rule(rule, dataset_metadata, dataset_id)

    def execute_rule(
        self,
        rule: dict,
        dataset_metadata: BaseDatasetMetadata,
        dataset_id: str,
    ) -> List[str]:
        """
        Executes the given rule on a given dataset (or a view of it).
        """
        expanded_rules = SQLRuleProcessor.expand_rule_for_variable_regex(rule, dataset_metadata.variables)
        results = []
        for expanded_rule in expanded_rules:
            results.extend(self._execute_expanded_rule(expanded_rule, dataset_metadata, dataset_id))
        return results

    def _execute_expanded_rule(
        self,
        rule: dict,
        dataset_metadata: BaseDatasetMetadata,
        dataset_id: str,
    ) -> List[str]:
        # Add conditions to rule for all variables if variables: all appears in condition
        rule_copy = deepcopy(rule)
        updated_conditions = SQLRuleProcessor.duplicate_conditions_for_all_targets(
            rule["conditions"],
            dataset_metadata.variables,
        )
        rule_copy["conditions"].set_conditions(updated_conditions)

        # Apply any operations
        operation_variables = SQLRuleProcessor.perform_rule_operations(
            rule_copy,
            dataset_id,
            dataset_metadata,
            data_service=self.data_service,
            standards_context=self.standards_context,
        )

        results = []
        result_handler = SqlVenmoResultHandler(
            results,
            dataset_metadata=dataset_metadata,
            rule=rule,
            data_service=self.data_service,
            dataset_id=dataset_id,
            operation_variables=operation_variables,
        )

        if self.compile_rules and self._run_compiled(
            rule_copy, dataset_metadata, dataset_id, operation_variables, result_handler
        ):
            return results

        # Translator between venmo and the check operators
        venmo_object = self._venmo_object(dataset_metadata, dataset_id, operation_variables)
        run(
            serialize_rule(rule_copy),
            defined_variables=venmo_object,
            defined_actions=result_handler,
        )
        return results

    def _venmo_object(
        self, dataset_metadata: BaseDatasetMetadata, dataset_id: str, operation_variables: dict, **params
    ) -> SqlVenmoObject:
        return SqlVenmoObject(
            dataset_id=dataset_id,
            data_service=self.data_service,
            column_prefix_map={"--": dataset_metadata.domain},
            operation_variables=operation_variables,
            dataset_metadata=dataset_metadata,
            **params,
        )

    def _run_compiled(
        self,
        rule: dict,
        dataset_metadata: BaseDatasetMetadata,
        dataset_id: str,
        operation_variables: dict,
        result_handler: SqlVenmoResultHandler,
    ) -> bool:
        actions = rule.get("actions") or []
        if len(actions) != 1 or actions[0].get("name") != "generate_dataset_error_objects":
            return False

        rule_label = f"Rule {rule.get('core_id')} on {dataset_metadata.name}"
        venmo_object = self._venmo_object(dataset_metadata, dataset_id, operation_variables, compile_only=True)
        try:
            truth = check_conditions_recursively(rule["conditions"].to_dict(), venmo_object)
        except CompileUnsupported as error:
            logger.debug(f"{rule_label} not compiled, running operator by operator: {error}")
            return False
        if isinstance(truth, bool):
            truth = SqlTruth("TRUE" if truth else "FALSE")
        if not isinstance(truth, SqlTruth):
            logger.debug(
                f"{rule_label} not compiled, running operator by operator: conditions gave {type(truth).__name__}"
            )
            return False

        try:
            failing_rows = result_handler.get_failing_rows(truth)
        except PostgresError as error:
            logger.warning(f"{rule_label} compiled query failed, rerunning operator by operator: {str(error).strip()}")
            return False
        if failing_rows:
            result_handler.report_failing_rows(**(actions[0].get("params") or {}), rows_with_error=failing_rows)
        return True

    # def get_define_xml_value_level_metadata(self, dataset_path: str, domain_name: str) -> List[dict]:
    #     """
    #     Gets Define XML variable metadata and returns it as dataframe.
    #     """
    #     define_xml_reader = DefineXMLReaderFactory.get_define_xml_reader(
    #         dataset_path, self.define_xml_path, self.data_service, self.cache
    #     )
    #     return define_xml_reader.extract_value_level_metadata(domain_name=domain_name)

    def handle_validation_exceptions(self, exception, name) -> ValidationErrorContainer:  # noqa
        if isinstance(exception, DatasetNotFoundError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error="Dataset Not Found",
                message=exception.message,
            )
            message = "rule execution error"
        elif isinstance(exception, RuleFormatError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error="Rule format error",
                message=exception.message,
            )
            message = "rule execution error"
        elif isinstance(exception, AssertionError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error="Rule format error",
                message="Rule contains invalid operator",
            )
            message = "rule execution error"
        elif isinstance(exception, ColumnNotFoundError):
            error_obj: ValidationErrorContainer = ValidationErrorContainer(
                status=ExecutionStatus.SKIPPED.value,
                dataset=name,
            )
            message = exception.message
            errors = [error_obj]
            return ValidationErrorContainer(
                dataset=name,
                errors=errors,
                message=message,
                status=ExecutionStatus.SKIPPED.value,
            )
        elif isinstance(exception, KeyError):
            message = ", ".join(sorted(exception.args[0].split(", ")))
            error_obj = FailedValidationEntity(
                dataset=name,
                error="Column not found in data",
                message=message,
            )
            message = "rule execution error"
        elif isinstance(exception, DomainNotFoundInDefineXMLError):
            message = ", ".join(sorted(exception.args[0].split(", ")))
            error_obj = FailedValidationEntity(
                dataset=name,
                error=DomainNotFoundInDefineXMLError.description,
                message=message,
            )
            message = "rule execution error"
        elif isinstance(exception, VariableMetadataNotFoundError):
            message = ", ".join(sorted(exception.args[0].split(", ")))
            error_obj = FailedValidationEntity(
                dataset=name,
                error=VariableMetadataNotFoundError.description,
                message=message,
            )
            message = "rule execution error"
        elif isinstance(exception, FailedSchemaValidation):
            if self.validate_xml:
                message = ", ".join(sorted(exception.args[0].split(", ")))
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    error=FailedSchemaValidation.description,
                    message=message,
                )
                message = "Schema Validation Error"
                errors = [error_obj]
                return ValidationErrorContainer(
                    errors=errors,
                    message=message,
                    status=ExecutionStatus.SUCCESS.value,
                    dataset=name,
                )
            else:
                error_obj: ValidationErrorContainer = ValidationErrorContainer(
                    status=ExecutionStatus.SKIPPED.value,
                    dataset=name,
                )
                message = "Skipped because schema validation is off"
                errors = [error_obj]
                return ValidationErrorContainer(
                    dataset=name,
                    errors=errors,
                    message=message,
                    status=ExecutionStatus.SKIPPED.value,
                )
        elif isinstance(exception, DomainNotFoundError):
            error_obj = ValidationErrorContainer(
                dataset=name,
                message=str(exception),
                status=ExecutionStatus.SKIPPED.value,
            )
            message = "rule evaluation skipped - operation domain not found"
            errors = [error_obj]
            return ValidationErrorContainer(
                dataset=name,
                errors=errors,
                message=message,
                status=ExecutionStatus.SKIPPED.value,
            )
        elif isinstance(exception, SqlOperatorError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error=f"SQL error in {exception.operator_name} operator",
                message=clean_postgres_message(str(exception.original_exception)),
            )
            message = "SQL operator execution error"
        elif isinstance(exception, SqlOperationError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error=f"SQL error in {exception.operation_name} operation",
                message=clean_postgres_message(str(exception.original_exception)),
            )
            message = "SQL operation execution error"
        elif isinstance(exception, ProgrammingError):
            error_obj = FailedValidationEntity(
                dataset=name,
                error="PostgreSQL Error",
                message=clean_postgres_message(str(exception)),
            )
            message = "SQL execution error"
        elif isinstance(exception, ValueError):
            error_message = str(exception)

            schema_pattern = r"Column\s+(\w+)\s+or\s+(\w+)\s+not found in the respective schemas"
            match = re.search(schema_pattern, error_message, re.IGNORECASE)

            if match:
                column_name = match.group(1).upper()

                error_obj = FailedValidationEntity(
                    dataset=name,
                    error="Column not found in data",
                    message=column_name,
                )
                message = "rule execution error"
            else:
                column_pattern = r"Column\s+['\"]?(\w+)['\"]?\s+(?:does not exist|not found|missing)"
                match = re.search(column_pattern, error_message, re.IGNORECASE)

                if match:
                    column_name = match.group(1).upper()
                    error_obj = FailedValidationEntity(
                        dataset=name,
                        error="Column not found in data",
                        message=column_name,
                    )
                    message = "rule execution error"
                else:
                    error_obj = FailedValidationEntity(
                        dataset=name,
                        error="Validation error",
                        message=error_message,
                    )
                    message = "rule execution error"
        else:
            error_obj = FailedValidationEntity(
                dataset=name,
                error="An unknown exception has occurred",
                message=str(exception),
            )
            message = "rule execution error"
        errors = [error_obj]
        return ValidationErrorContainer(
            dataset=name,
            errors=errors,
            message=message,
            status=ExecutionStatus.EXECUTION_ERROR.value,
        )
