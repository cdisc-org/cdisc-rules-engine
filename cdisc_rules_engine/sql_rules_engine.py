import re
import traceback
from copy import deepcopy
from typing import Callable, List, Tuple, Union

from psycopg2 import Error as PostgresError
from psycopg2.errors import ProgrammingError

from cdisc_rules_engine.compiler.compiler import CompiledQuery, RuleQueryCompiler
from cdisc_rules_engine.compiler.conditions import ConditionCompiler
from cdisc_rules_engine.compiler.sources import rule_source
from cdisc_rules_engine.constants.rule_constants import MAX_ERRORS_PER_DATASET
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
from cdisc_rules_engine.models.sql_result_handler import SqlResultHandler
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlOperationError
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.utilities.rule_progress import (
    DisabledRuleProgressReporter,
    RuleProgressPhase,
    RuleProgressReporter,
)
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor


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
        max_errors_per_dataset: int = MAX_ERRORS_PER_DATASET,
    ):
        self.data_service = data_service
        self.standards_context = standards_context
        self.progress_reporter = progress_reporter or DisabledRuleProgressReporter()
        self.max_errors_per_dataset = max_errors_per_dataset

    def sql_validate_single_rule(self, rule: dict, not_applicable_reason: str = None):
        """
        Runs the rule against every uploaded dataset in its scope. If not_applicable_reason is given
        (e.g. the rule needs a define.xml that wasn't supplied), the rule isn't run and each dataset
        in scope is reported as skipped with that reason.
        """
        results = {}
        self.progress_reporter.set_phase(RuleProgressPhase.PARSING)
        rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
        is_study_sensitivity = rule.get("sensitivity") == Sensitivity.STUDY.value
        study_error_already_reported = False

        # Collect all dataset metadata for rule types that read every dataset (e.g. Domain Presence Check)
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
            if is_suitable and not_applicable_reason:
                results[dataset_metadata.name] = self._containers_per_reported_file(
                    dataset_metadata,
                    lambda filename: ValidationErrorContainer(
                        status=ExecutionStatus.SKIPPED.value,
                        message=not_applicable_reason,
                        dataset=filename,
                        domain=dataset_metadata.domain,
                    ),
                )
            elif is_suitable:
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
        It gets the rows the rule's type checks and then executes the rule against them.
        """
        self.progress_reporter.set_phase(RuleProgressPhase.EXECUTING)
        dataset_id = rule_source(rule, self.data_service, dataset_metadata, self.standards_context, datasets)

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

        compiler = RuleQueryCompiler(
            rule_copy,
            self.data_service,
            dataset_metadata,
            dataset_id,
            operation_variables,
            target_rule=rule,
            max_errors=self.max_errors_per_dataset,
        )
        compiled, rows = self._failing_rows(compiler)

        results = []
        SqlResultHandler(
            results,
            dataset_metadata=dataset_metadata,
            rule=rule,
            data_service=self.data_service,
            dataset_id=dataset_id,
            operation_variables=operation_variables,
        ).report(compiled, rows)
        return results

    def _failing_rows(self, compiler: RuleQueryCompiler) -> Tuple[CompiledQuery, List[dict]]:
        """
        Run the compiled rule and return a tuple of the query that actually ran and its rows.
        First try selects operation outputs inside the query. If the cursor rejects that then we
        recompile without them and let the result handler evaluate them per row. This is ARGUABLY
        slower, but it's what the old engine did anyway. A failure with nothing inlined is a
        real error in one of the checks.
        """
        compiled = compiler.compile()
        try:
            return compiled, self._fetch(compiled.sql)
        except PostgresError as error:
            if not compiled.inline_outputs:
                self._raise_for_failing_check(error, compiler)
                raise
            logger.warning(
                f"Rule {compiler.rule.get('core_id')} on {compiler.dataset_metadata.name}: operation outputs "
                f"could not be selected in the query, evaluating them per row: {str(error).strip()}"
            )

        compiled = compiler.compile(inline_outputs=False)
        try:
            return compiled, self._fetch(compiled.sql)
        except PostgresError as error:
            self._raise_for_failing_check(error, compiler)
            raise

    def _fetch(self, sql: str) -> List[dict]:
        self.progress_reporter.set_phase(RuleProgressPhase.EXECUTING)
        self.data_service.pgi.execute_sql(sql, log_errors=False)
        rows = self.data_service.pgi.fetch_all()
        self.progress_reporter.set_phase(RuleProgressPhase.EVALUATING)
        return rows

    def _raise_for_failing_check(self, error: PostgresError, compiler: RuleQueryCompiler) -> None:
        """
        The whole rule is one statement so a pg query error doesn't say which check broke it annoyingly.
        Sooooo... we re-run the checks one at a time and blame the first one that fails.
        This way the user sees "error in operator A" rather than a wall of generated sql.
        If every check runs fine alone (e.g. it's the combination, or the reporting part of the
        query), this just logs and returns, and the caller re-raises the original error.
        """
        rule, dataset_metadata, dataset_id = compiler.rule, compiler.dataset_metadata, compiler.dataset_id
        operation_variables = compiler.operation_variables
        table_hash = self.data_service.pgi.schema.get_table_hash(dataset_id)
        for condition in rule["conditions"].values():
            operator = condition.get("operator")
            compiled = ConditionCompiler(
                self.data_service, dataset_id, dataset_metadata, operation_variables
            ).compile_condition(operator, condition.get("value", {}))
            try:
                self.data_service.pgi.execute_sql(
                    f"SELECT COUNT(*) AS count FROM {compiled.from_clause(table_hash)} WHERE {compiled.where}",
                    log_errors=False,
                )
            except PostgresError as e:
                raise SqlOperatorError(original_exception=e, operator_name=operator) from error
        logger.error(f"Compiled query for rule {rule.get('core_id')} on {dataset_metadata.name} failed: {error}")

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
