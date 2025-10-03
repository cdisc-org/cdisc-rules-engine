from typing import List, Union
from dataclasses import dataclass
from cdisc_rules_engine.utilities.utils import get_execution_status

from .base_validation_entity import BaseValidationEntity
from .failed_validation_entity import FailedValidationEntity
from .validation_error_entity import ValidationErrorEntity


@dataclass
class ValidationErrorContainer(BaseValidationEntity):
    dataset: str | None = None
    domain: str | None = None
    targets: List[str] = None
    errors: List[Union[ValidationErrorEntity, FailedValidationEntity]] = None
    message: str | None = None

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
