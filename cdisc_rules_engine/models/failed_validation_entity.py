from dataclasses import dataclass, field
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from .base_validation_entity import BaseValidationEntity


@dataclass
class FailedValidationEntity(BaseValidationEntity):
    """
    The entity describes an error that occurred during validation
    indicating that the process has finished its execution with error.
    """

    dataset: str = field(init=False)
    _error: str = field(init=False)
    _message: str = field(init=False)
    
    def __init__(self, error: str, message: str, dataset: str):
        super().__init__()
        self.dataset = dataset
        self._error = error
        self._message = message
        self.status = ExecutionStatus.EXECUTION_ERROR

    def as_dict(self) -> dict:
        return {
            "dataset": self.dataset,
            "error": self._error,
            "message": self._message,
        }
