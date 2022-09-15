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

    def values(self) -> List[dict]:
        """
        Returns the condition as a list of dictionaries.
        """
        return [self.to_dict()]

    def items(self) -> List[tuple]:
        return self._condition.items()

    def duplicate(self, targets: List[str]) -> List[ConditionInterface]:
        conditions: List[SingleCondition] = []
        if self.should_duplicate():
            for target in targets:
                new_condition = deepcopy(self._condition)
                new_condition["value"] = new_condition.get("value", {})
                new_condition["value"]["target"] = target
                conditions.append(SingleCondition(new_condition))
        else:
            conditions.append(self)
        return conditions

    def should_duplicate(self) -> bool:
        return "target" not in self._condition.get("value", {})

    def add_operator(self, operator):
        self._condition["operator"] = operator
