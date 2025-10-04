from dataclasses import dataclass
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from .base_validation_entity import BaseValidationEntity


@dataclass
class FailedValidationEntity(BaseValidationEntity):
    """
    The entity describes an error that occurred during validation
    indicating that the process has finished its execution with error.
    """
    error: str
    message: str
    dataset: str
    status: ExecutionStatus = ExecutionStatus.EXECUTION_ERROR
