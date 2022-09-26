from typing import List
from copy import deepcopy
from cdisc_rules_engine.interfaces import ConditionInterface


class SingleCondition(ConditionInterface):
    """
    Represents a single condition like:
    {
        "name": "get_dataset",
        "operator": "suffix_not_equal_to",
        "value": {
            "target": "$dataset_name",
            "comparator": "RDOMAIN",
            "suffix": 2
        }
    }
    """

    def __init__(self, condition: dict):
        self._condition = condition

    def to_dict(self) -> dict:
        return self._condition

    def set_target(self, target) -> "ConditionInterface":
        self._condition["value"] = self._condition.get("value", {})
        self._condition["value"]["target"] = target
        return self

    def set_conditions(self, conditions: dict):
        self._condition = conditions

    def values(self) -> List[dict]:
        """
        Returns the condition as a list of dictionaries.
        """
        return [self.to_dict()]

    def items(self) -> List[tuple]:
        return self._condition.items()

    def copy(self) -> ConditionInterface:
        return SingleCondition(deepcopy(self._condition))

    def should_copy(self) -> bool:
        return "target" not in self._condition.get("value", {})

    def get_conditions(self) -> dict:
        return self._condition
