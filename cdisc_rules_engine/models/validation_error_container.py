from typing import List
from dataclasses import dataclass, field
from cdisc_rules_engine.utilities.utils import get_execution_status

from .base_validation_entity import BaseValidationEntity
from .failed_validation_entity import FailedValidationEntity
from .validation_error_entity import ValidationErrorEntity


@dataclass
class ValidationErrorContainer(BaseValidationEntity):
    dataset: str | None = None
    domain: str | None = None
    targets: List[str] = field(default_factory=list)
    errors: List[ValidationErrorEntity | FailedValidationEntity] = field(
        default_factory=list
    )
    message: str | None = None
    status: str | None = None
    entity: str | None = None

    @property
    def executionStatus(self):
        return self.status or get_execution_status(self.errors)

    def to_representation(self) -> dict:
        return {
            "executionStatus": self.executionStatus,
            "dataset": self.dataset,
            "domain": self.domain,
            "variables": sorted(self.targets),
            "message": self.message,
            "errors": [error.to_representation() for error in self.errors],
            **({"entity": self.entity} if self.entity else {}),
        }
