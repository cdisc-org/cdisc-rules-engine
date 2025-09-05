from functools import cache
from glob import glob
from jsonata import Jsonata

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
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
        dataset_metadata: SDTMDatasetMetadata,
        jsonata_functions_path: str,
    ):
        custom_functions = JSONataProcessor.get_custom_functions(jsonata_functions_path)
        check = rule.get("conditions")
        full_string = f"(\n{custom_functions}{check}\n)"
        expr = Jsonata(full_string)
        results = expr.evaluate(dataset)
        errors = (
            [
                ValidationErrorEntity(
                    value=result,
                    dataset=dataset_metadata.name,
                    row=result.get("path"),
                    usubjid=result.get("id"),
                    sequence=result.get("iid"),
                )
                for result in results
            ]
            if results
            else []
        )
        validation_error_container = ValidationErrorContainer(
            dataset=dataset_metadata.name,
            domain=dataset_metadata.domain,
            targets=rule.get("output_variables"),
            errors=errors,
            message=next(iter(rule.get("actions", [])), {})
            .get("params", {})
            .get("message"),
            status=(
                ExecutionStatus.SUCCESS.value
                if results
                else ExecutionStatus.EXECUTION_ERROR.value
            ),
        )
        return [validation_error_container.to_representation()]

    @staticmethod
    @cache
    def get_custom_functions(jsonata_functions_path):
        if not jsonata_functions_path:
            return ""
        functions = []
        for filepath in glob(f"{jsonata_functions_path}/*.jsonata"):
            with open(filepath, "r") as file:
                function_definition = file.read()
                function_definition = function_definition.replace("{", "", 1)
                function_definition = "".join(function_definition.rsplit("}", 1))
                functions.append(function_definition)
        functions_str = ",\n".join(functions)
        return f"$utils:={{\n{functions_str}\n}};\n"
