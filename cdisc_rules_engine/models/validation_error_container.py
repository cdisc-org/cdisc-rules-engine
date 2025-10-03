from typing import List, Union, Optional
from dataclasses import dataclass, field

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.utilities.utils import get_execution_status

from .base_validation_entity import BaseValidationEntity
from .failed_validation_entity import FailedValidationEntity
from .validation_error_entity import ValidationErrorEntity


@dataclass
class ValidationErrorContainer(BaseValidationEntity):
    dataset: Optional[str] = field(default=None, init=False)
    domain: Optional[str] = field(default=None, init=False)
    targets: List[str] = field(default_factory=list, init=False)
    errors: List[Union[ValidationErrorEntity, FailedValidationEntity]] = field(default_factory=list, init=False)
    message: Optional[str] = field(default=None, init=False)
    
    def __init__(self, **params):
        super().__init__()
        self.dataset = params.get("dataset")
        self.domain = params.get("domain")
        self.targets = params.get("targets", [])
        self.errors = params.get("errors", [])
        self.message = params.get("message")
        self.status = params.get("status") or get_execution_status(self.errors)

    def as_dict(self) -> dict:
        return {
            "executionStatus": self.status,
            "dataset": self.dataset,
            "domain": self.domain,
            "variables": sorted(self.targets),
            "message": self.message,
            "errors": [error.as_dict() for error in self.errors],
        }
