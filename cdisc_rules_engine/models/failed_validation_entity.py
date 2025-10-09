from dataclasses import dataclass
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from .base_validation_entity import BaseValidationEntity


@dataclass(kw_only=True)
class FailedValidationEntity(BaseValidationEntity):
    """
    The entity describes an error that occurred during validation
    indicating that the process has finished its execution with error.
    """

    dataset: str
    error: str
    message: str
    status: ExecutionStatus = ExecutionStatus.EXECUTION_ERROR

    def to_representation(self) -> dict:
        return {
            "dataset": self.dataset,
            "error": self.error,
            "message": self.message,
        }
