from abc import ABC, abstractmethod


class DictionaryTermInterface(ABC):
    """
    Interface that must be implemented by
    all dictionary terms
    """

    @abstractmethod
    def get_identifier(self) -> str:
        """
        Returns string identifier for the term
        """
