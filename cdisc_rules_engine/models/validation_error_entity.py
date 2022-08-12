from cdisc_rules_engine.enums.execution_status import ExecutionStatus

from .base_validation_entity import BaseValidationEntity


class ValidationErrorEntity(BaseValidationEntity):
    """
    The entity describes an error that been flagged because
    a dataset violates a rule in a certain row.
    """

    def __init__(
        self, value: dict, row: int = None, usubjid: str = None, sequence: int = None
    ):
        self._row: int = row
        self.value: dict = value
        self._usubjid: str = usubjid
        self._sequence: int = sequence
        self.status: ExecutionStatus = ExecutionStatus.SUCCESS

    def to_representation(self) -> dict:
        representation: dict = {
            "value": self.value,
        }
        if self._row is not None:
            representation["row"] = self._row
        if self._usubjid:
            representation["uSubjId"] = self._usubjid
        if self._sequence:
            representation["seq"] = self._sequence
        return representation
