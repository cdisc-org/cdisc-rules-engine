from abc import ABC, abstractmethod


class RepresentationInterface(ABC):
    """
    Interface that should be implemented
    by all objects that are later represented.
    """

    @abstractmethod
    def to_representation(self) -> dict:
        """
        Returns the contents as dict.
        """
