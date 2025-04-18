from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.base_enum import BaseEnum
from .base_validation_entity import BaseValidationEntity


class ValidationErrorEntity(BaseValidationEntity):
    """
    The entity describes an error that been flagged because
    a dataset violates a rule in a certain row.
    """

    def __init__(
        self,
        value: dict,
        dataset: str = None,
        row: int = None,
        usubjid: str = None,
        sequence: int = None,
    ):
        self._dataset: str = dataset
        self._row: int = row
        self.value: dict = value
        self._usubjid: str = usubjid
        self._sequence: int = sequence
        self.status: ExecutionStatus = ExecutionStatus.SUCCESS

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
        if self._dataset is not None:
            representation["dataset"] = self._dataset
        if self._row is not None:
            representation["row"] = self._row
        if self._usubjid:
            representation["USUBJID"] = self._usubjid
        if self._sequence:
            representation["SEQ"] = self._sequence
        return representation
