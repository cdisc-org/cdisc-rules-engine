from abc import ABC, abstractmethod
from typing import List


class ConditionInterface(ABC):
    """
    Interface that must be implemented by
    all elements of a conditions composite.
    """

    @abstractmethod
    def to_dict(self) -> dict:
        """
        Serializes a condition into a dict.
        """

    @abstractmethod
    def values(self) -> List[dict]:
        """
        Returns all conditions of a node or a composite
        as a list of dictionaries.
        """

    @abstractmethod
    def items(self) -> List[tuple]:
        """
        Returns an iterable of tuples.
        """

    @abstractmethod
    def copy(self) -> "ConditionInterface":
        """
        Returns a copy of the current condition interface
        """

    @abstractmethod
    def should_copy(self) -> bool:
        """
        Returns true if the condition inteface should be copied.
        This is true if a condition is supposed to be run on all variables.
        """

    @abstractmethod
    def get_conditions(self) -> dict:
        """
        Returns the raw conditons dictionary mapping key to condition interface
        """

    @abstractmethod
    def set_target(self, target) -> "ConditionInterface":
        """
        Updates the target of a condition
        """

    @abstractmethod
    def set_conditions(self, conditions: dict):
        """
        Updates the conditions of a condition interface
        """
