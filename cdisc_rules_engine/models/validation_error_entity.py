from dataclasses import dataclass, field

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.base_enum import BaseEnum
from .base_validation_entity import BaseValidationEntity


@dataclass
class ValidationErrorEntity(BaseValidationEntity):
    """
    The entity describes an error that been flagged because
    a dataset violates a rule in a certain row.
    """

    value: dict = field(default_factory=dict)
    dataset: str | None = None
    row: int | None = None
    USUBJID: str | None = None
    SEQ: int | None = None
    status: ExecutionStatus = ExecutionStatus.SUCCESS
    entity: str | None = None
    instance_id: str | None = None
    path: str | None = None

    def _format_values(self) -> dict:
        """
        Converts values to json serializable dict
        """
        data = {}
        for key, val in self.value.items():
            if isinstance(val, set):
                data[key] = list(val)
            elif isinstance(val, BaseEnum):
                data[key] = val.value
            else:
                data[key] = val
        return data

    def to_representation(self) -> dict:
        representation: dict = {
            "value": self._format_values(),
        }
        if self.dataset is not None:
            representation["dataset"] = self.dataset
        if self.row is not None:
            representation["row"] = self.row
        if self.USUBJID is not None:
            representation["USUBJID"] = self.USUBJID
        if self.SEQ is not None:
            representation["SEQ"] = self.SEQ
        if self.entity is not None:
            representation["entity"] = self.entity
        if self.instance_id is not None:
            representation["instance_id"] = self.instance_id
        if self.path is not None:
            representation["path"] = self.path
        return representation
