from abc import ABC, abstractmethod


class ConfigInterface(ABC):
    """
    The interface defines a set of methods
    that must be implemented by all custom configuration
    classes.
    """

    def __init__(self):
        self.configs = {}

    @abstractmethod
    def getValue(self, key: str, default=None):
        """
        Returns value of a configuration parameter.
        """
        if key in self.configs:
            return self.configs[key]
        return None

    def setValue(self, key: str, value: str):
        self.configs[key] = value
