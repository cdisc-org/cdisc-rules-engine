from dataclasses import dataclass
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from .base_validation_entity import BaseValidationEntity


@dataclass
class FailedValidationEntity(BaseValidationEntity):
    """
    The entity describes an error that occurred during validation
    indicating that the process has finished its execution with error.
    """
    # Required fields first (no defaults)
    error: str
    message: str
    dataset: str
    # Optional fields with defaults
    status: ExecutionStatus = ExecutionStatus.EXECUTION_ERROR
