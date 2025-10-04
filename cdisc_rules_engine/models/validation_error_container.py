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
            self._status = ExecutionStatus(get_execution_status(self.errors))

    @property
    def status(self) -> ExecutionStatus:
        if self._status is not None:
            return self._status
        return ExecutionStatus(get_execution_status(self.errors))

    @status.setter
    def status(self, value: ExecutionStatus | str):
        if isinstance(value, str):
            self._status = ExecutionStatus(value)
        else:
            self._status = value

    def as_dict(self) -> dict:
        return {
            "executionStatus": self.status,
            "dataset": self.dataset,
            "domain": self.domain,
            "variables": sorted(self.targets),
            "message": self.message,
            "errors": [error.as_dict() for error in self.errors],
        }
