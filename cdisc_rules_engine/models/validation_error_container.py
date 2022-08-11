from typing import List, Union

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.utilities.utils import get_execution_status

from .base_validation_entity import BaseValidationEntity
from .failed_validation_entity import FailedValidationEntity
from .validation_error_entity import ValidationErrorEntity


class ValidationErrorContainer(BaseValidationEntity):
    def __init__(self, **params):
        self.domain: str = params.get("domain")
        self.targets: List[str] = params.get("targets", [])
        self.errors: List[
            Union[ValidationErrorEntity, FailedValidationEntity]
        ] = params.get("errors", [])
        self.message: str = params.get("message")
        self.status: ExecutionStatus = params.get("status") or get_execution_status(
            self.errors
        )

    def to_representation(self) -> dict:
        return {
            "executionStatus": self.status,
            "domain": self.domain,
            "variables": sorted(self.targets),
            "message": self.message,
            "errors": [error.to_representation() for error in self.errors],
        }
