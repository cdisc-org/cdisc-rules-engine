from abc import ABC, abstractmethod


class ConfigInterface(ABC):
    """
    The interface defines a set of methods
    that must be implemented by all custom configuration
    classes.
    """

    @abstractmethod
    def getValue(self, key: str, default=None):
        """
        Returns value of a configuration parameter.
        """
