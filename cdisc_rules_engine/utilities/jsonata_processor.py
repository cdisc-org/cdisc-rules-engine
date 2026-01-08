from collections import defaultdict
from functools import cache
from glob import glob
from jsonata import Jsonata

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.exceptions.custom_exceptions import (
    MissingDataError,
    RuleExecutionError,
    RuleFormatError,
)
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.models.validation_error_entity import (
    ValidationErrorEntity,
)


class JSONataProcessor:

    @staticmethod
    def execute_jsonata_rule(
        rule: dict,
        dataset: dict,
        jsonata_custom_functions: tuple[()] | tuple[tuple[str, str], ...],
    ):
        custom_functions = JSONataProcessor.get_all_custom_functions(
            jsonata_custom_functions
        )
        check = rule.get("conditions")
        full_string = f"(\n{custom_functions}{check}\n)"
        try:
            expr = Jsonata(full_string)
        except Exception as e:
            raise RuleFormatError(
                f"\n  Error parsing JSONata Rule for Core Id: {rule.get('core_id')}"
                f"\n  {type(e).__name__}: {e}"
            )
        try:
            results = expr.evaluate(dataset)
        except Exception as e:
            raise RuleExecutionError(
                f"\n  Error evaluating JSONata Rule with Core Id: {rule.get('core_id')}"
                f"\n  {type(e).__name__}: {e}"
            )
        errors = defaultdict(list)
        if results:
            if isinstance(results, dict):
                results = [results]
            if not isinstance(results, list):
                raise RuleFormatError(
                    f"\n  Error in return type of JSONata Rule with Core Id: {rule.get('core_id')}"
                    f"\n  Expected a list, but got: {results}"
                )
            for result in results:
                entity = (
                    result.get("entity")
                    or result.get("dataset")
                    or result.get("instanceType")
                )
                error_entity = ValidationErrorEntity(
                    value=result,
                    dataset=entity,
                    row=result.get("row"),
                    USUBJID=result.get("USUBJID"),
                    SEQ=result.get("SEQ"),
                    entity=entity,
                    instance_id=result.get("instance_id") or result.get("id"),
                    path=result.get("path") or result.get("_path"),
                )
                errors[entity].append(error_entity)
        validation_error_container = [
            ValidationErrorContainer(
                dataset=entity,
                domain=entity,
                targets=rule.get("output_variables"),
                errors=error,
                message=next(iter(rule.get("actions", [])), {})
                .get("params", {})
                .get("message"),
                status=(
                    ExecutionStatus.SUCCESS.value
                    if results
                    else ExecutionStatus.EXECUTION_ERROR.value
                ),
                entity=entity,
            ).to_representation()
            for entity, error in errors.items()
        ]
        return validation_error_container

    @staticmethod
    @cache
    def get_all_custom_functions(
        jsonata_custom_functions: tuple[()] | tuple[tuple[str, str], ...]
    ):
        builtins_and_customs = [
            ("utils", DefaultFilePaths.JSONATA_UTILS.value),
            *jsonata_custom_functions,
        ]
        functions = [
            JSONataProcessor.get_custom_functions(name, path)
            for name, path in builtins_and_customs
        ]
        return "\n".join(functions)

    @staticmethod
    def get_custom_functions(jsonata_functions_name: str, jsonata_functions_path: str):
        functions = []
        filepaths = glob(f"{jsonata_functions_path}/*.jsonata")
        if not filepaths:
            raise MissingDataError(
                f"\n  No JSONata custom functions found at path: {jsonata_functions_path}"
            )
        for filepath in filepaths:
            try:
                with open(filepath, "r") as file:
                    function_definition = file.read()
                    function_definition = function_definition.replace("{", "", 1)
                    function_definition = "".join(function_definition.rsplit("}", 1))
                    functions.append(function_definition)
            except Exception as e:
                raise RuleFormatError(
                    f"\n  Error loading JSONata custom functions at path: {filepath}"
                    f"\n  {type(e).__name__}: {e}"
                )
        functions_str = ",\n".join(functions)
        return f"${jsonata_functions_name}:={{\n{functions_str}\n}};\n"
