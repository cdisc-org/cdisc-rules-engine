from typing import List
from dataclasses import dataclass, field
from cdisc_rules_engine.utilities.utils import get_execution_status
from cdisc_rules_engine.enums.execution_status import ExecutionStatus

from .base_validation_entity import BaseValidationEntity
from .failed_validation_entity import FailedValidationEntity
from .validation_error_entity import ValidationErrorEntity


@dataclass
class ValidationErrorContainer(BaseValidationEntity):
    dataset: str | None = None
    domain: str | None = None
    targets: List[str] = field(default_factory=list)
    errors: List[ValidationErrorEntity | FailedValidationEntity] = field(default_factory=list)
    message: str | None = None
    _status: ExecutionStatus | None = field(default=None, repr=False)

    def __post_init__(self):
        # If no explicit status was set, compute it from errors
        if self._status is None:
            status_value = get_execution_status(self.errors)
            # Find the ExecutionStatus enum by its string value
            self._status = ExecutionStatus.SUCCESS  # Default fallback
            if status_value:  # Add None check for safety
                for status in ExecutionStatus:
                    if status.value == status_value:
                        self._status = status
                        break

    @property
    def status(self) -> ExecutionStatus:
        if self._status is not None:
            return self._status
        status_value = get_execution_status(self.errors)
        # Find the ExecutionStatus enum by its string value
        result = ExecutionStatus.SUCCESS  # Default fallback
        if status_value:  # Add None check for safety
            for status in ExecutionStatus:
                if status.value == status_value:
                    result = status
                    break
        return result

    @status.setter
    def status(self, value: ExecutionStatus | str):
        if isinstance(value, str):
            # Find the ExecutionStatus enum by its string value
            self._status = ExecutionStatus.SUCCESS  # Default fallback
            if value:  # Add None/empty string check for safety
                for status in ExecutionStatus:
                    if status.value == value:
                        self._status = status
                        break
        else:
            self._status = value

    def to_representation(self) -> dict:
        return {
            "executionStatus": self.status,
            "dataset": self.dataset,
            "domain": self.domain,
            "variables": sorted(self.targets),
            "message": self.message,
            "errors": [error.to_representation() for error in self.errors],
        }
