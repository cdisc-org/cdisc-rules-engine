from typing import List

from cdisc_rules_engine.interfaces import ConditionInterface


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

    def get_conditions(self) -> dict:
        return self._conditions

    def set_target(self, target) -> "ConditionInterface":
        for key, conditions_list in self._conditions.items():
            new_conditions_list = []
            for condition in conditions_list:
                new_conditions_list.append(condition.copy().set_target(target))
            self._conditions[key] = new_conditions_list
        return self

    def set_conditions(self, conditions: dict):
        self._conditions = conditions

    def copy(self):
        new_condition = ConditionComposite()
        for key, condition in self._conditions.items():
            new_condition.add_conditions(key, condition)
        return new_condition

    def add_conditions(self, key: str, conditions: List[ConditionInterface]):
        """
        Adds a list of objects with ConditionInterface
        interface to the given key.
        """
        self._conditions[key] = conditions

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

    def should_copy(self) -> bool:
        duplicate = False
        for key, conditions in self._conditions.items():
            for condition in conditions:
                duplicate = duplicate or condition.should_copy()
        return duplicate
