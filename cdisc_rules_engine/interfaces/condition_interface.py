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
