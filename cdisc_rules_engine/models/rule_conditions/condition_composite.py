from typing import List

from cdisc_rules_engine.interfaces import ConditionInterface
from cdisc_rules_engine.models.rule_conditions.single_condition import SingleCondition


class ConditionComposite(ConditionInterface):
    """
    This class contains a condition that can include
    nested conditions like:
    {
        "all": [
            {
                "any": [
                    {"value": {"target": "dataset_name"}},
                    {"value": {"target": "dataset_label"}},
                    {"all": [{"value": {"target": "dataset_location"}}]},
                ]
            },
            {"value": {"target": "dataset_location"}},
        ]
    }
    """

    def __init__(self):
        self._conditions = {}

    def add_conditions(self, key: str, conditions: List[ConditionInterface]):
        """
        Adds a list of objects with ConditionInterface
        interface to the given key.
        """
        self._conditions[key] = conditions

    def add_conditions_for_targets(self, targets: List[str]):
        """
        If a condition specifies the parameter variable: "all",
        the condition will be duplicated for all targets.
        """
        for key, conditions in self._conditions.items():
            conditions_to_add = []
            for cond in conditions:
                if isinstance(cond, SingleCondition):
                    conditions_to_add.extend(cond.duplicate(targets))
                else:
                    conditions_to_add.append(cond.add_conditions_for_targets(targets))
            self.add_conditions(key, conditions_to_add)
        return self

    def to_dict(self) -> dict:
        """
        Serializes all nested conditions into a dict.
        """
        representation: dict = {}
        for key, condition_list in self._conditions.items():
            representation[key] = [condition.to_dict() for condition in condition_list]
        return representation

    def values(self) -> List[dict]:
        """
        Returns the nested conditions
        as a list of dictionaries.
        All nested conditions are recursively unpacked into
        a one-dimensional list. It is convenient when there is
        a need to process all values.
        """
        values = []
        for key, condition_list in self._conditions.items():
            for condition in condition_list:
                values.extend(condition.values())
        return values

    def items(self) -> List[tuple]:
        """
        Returns a list of tuples for each nested condition like:
        [
            ("all", [{"operator": "empty", "name": "get_dataset"}]),
            (
                "any",
                [
                    {
                        "operator": "empty",
                        "name": "get_dataset"
                    },
                    {
                        "equal_to": "empty",
                        "name": "get_dataset",
                        "value": {"comparator": 100}
                    }
                ]
            ),
            ...
        ]
        """
        items = []
        for key, condition_list in self._conditions.items():
            items.append((key, [condition.to_dict() for condition in condition_list]))
        return items
