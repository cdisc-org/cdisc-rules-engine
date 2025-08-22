from jsonata import Jsonata

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.models.validation_error_entity import (
    ValidationErrorEntity,
)


class JSONataProcessor:

    @staticmethod
    def execute_jsonata_rule(rule, dataset, datasets, dataset_metadata, **kwargs):
        check = rule.get("conditions")
        expr = Jsonata(check)
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
            message=rule.get("message"),
            status=(
                ExecutionStatus.SUCCESS.value
                if results
                else ExecutionStatus.EXECUTION_ERROR.value
            ),
        )
        return [validation_error_container.to_representation()]
