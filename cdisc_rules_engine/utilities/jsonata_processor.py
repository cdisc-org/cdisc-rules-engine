from collections import defaultdict
from functools import cache
from glob import glob
from jsonata import Jsonata

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.exceptions.custom_exceptions import (
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
        jsonata_functions_path: str,
    ):
        custom_functions = JSONataProcessor.get_custom_functions(jsonata_functions_path)
        check = rule.get("conditions")
        full_string = f"(\n{custom_functions}{check}\n)"
        expr = Jsonata(full_string)
        try:
            results = expr.evaluate(dataset)
        except Exception as e:
            raise RuleFormatError(
                f"\n  Error evaluating JSONata Rule with Core Id: {rule.get("core_id")}"
                f"\n  {type(e).__name__}: {e}"
            )
        errors = defaultdict(list)
        if results:
            if not isinstance(results, list):
                raise RuleFormatError(
                    f"\n  Error in return type of JSONata Rule with Core Id: {rule.get('core_id')}"
                    f"\n  Expected a list, but got: {results}"
                )
            for result in results:
                error_entity = ValidationErrorEntity(
                    value=result,
                    dataset=result.get("dataset") or "",
                    row=result.get("record"),
                    usubjid=result.get("usubjid"),
                    sequence=result.get("sequence"),
                )
                errors[result.get("dataset")].append(error_entity)
        validation_error_container = [
            ValidationErrorContainer(
                dataset=dataset,
                domain=dataset,
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
            ).to_representation()
            for dataset, error in errors.items()
        ]
        return validation_error_container

    @staticmethod
    @cache
    def get_custom_functions(jsonata_functions_path):
        if not jsonata_functions_path:
            return ""
        functions = []
        for filepath in glob(f"{jsonata_functions_path}/*.jsonata"):
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
        return f"$utils:={{\n{functions_str}\n}};\n"
