from cdisc_rules_engine.enums.execution_status import ExecutionStatus

from .base_validation_entity import BaseValidationEntity


class FailedValidationEntity(BaseValidationEntity):
    """
    The entity describes an error that occurred during validation
    indicating that the process has finished its execution with error.
    """

    def __init__(self, error: str, message: str):
        self._error = error
        self._message = message
        self.status = ExecutionStatus.EXECUTION_ERROR

    def to_representation(self) -> dict:
        return {
            "error": self._error,
            "message": self._message,
        }
