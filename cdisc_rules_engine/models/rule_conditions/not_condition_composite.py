from typing import List

from cdisc_rules_engine.interfaces import ConditionInterface


class NotConditionComposite(ConditionInterface):
    """
    Represents a "not" condition.
    "not" condition wraps a usual composite and looks like:
    {
        "not": {
            "all": [
                {
                    "any": [
                        {"value": {"target": "dataset_name"}},
                        {"value": {"target": "dataset_label"}},
                    ]
                },
                {"value": {"target": "dataset_location"}},
            ]
        },
    }
    """

    def __init__(self, key: str, condition_composite: ConditionInterface):
        self._key = key
        self._condition_composite = condition_composite

    def to_dict(self) -> dict:
        """
        Serializes all nested conditions into a dict.
        """
        return {self._key: self._condition_composite.to_dict()}

    def values(self) -> List[dict]:
        """
        Returns the list of nested conditions
        as a list of dictionaries.
        """
        return self._condition_composite.values()

    def items(self) -> List[tuple]:
        return self._condition_composite.items()

    def add_conditions_for_targets(self, targets: List[str]):
        self._condition_composite = (
            self._condition_composite.add_conditions_for_targets(targets)
        )
        return self

    def copy(self) -> "NotConditionComposite":
        composite_copy = self._condition_composite.copy()
        return NotConditionComposite(self._key, composite_copy)

    def should_copy(self) -> bool:
        return self._condition_composite.should_copy()

    def get_conditions(self) -> dict:
        return self._condition_composite.get_conditions()

    def set_target(self, target) -> ConditionInterface:
        self._condition_composite.set_target(target)
        return self

    def set_conditions(self, conditions: dict):
        self._condition_composite.set_conditions(conditions)
