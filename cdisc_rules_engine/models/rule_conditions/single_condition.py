from typing import List

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
        return self.items()
