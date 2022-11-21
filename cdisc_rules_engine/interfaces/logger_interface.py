from abc import ABC, abstractmethod

from .config_interface import ConfigInterface


class LoggerInterface(ABC):
    """
    This interface describes a set of methods
    which must be implemented by all custom loggers.
    """

    @classmethod
    @abstractmethod
    def get_instance(cls, config: ConfigInterface) -> "LoggerInterface":
        """
        Creates a new instance.
        """

    @property
    @abstractmethod
    def disabled(self) -> bool:
        """
        Returns if the logger is disabled or no.
        """

    @disabled.setter
    @abstractmethod
    def disabled(self, value: bool):
        """
        Used to disable the logger.
        """

    @abstractmethod
    def setLevel(self, level: str):
        """
        Sets log level.
        The method is called using camelCase to keep
        the interface similar to logging library.
        """

    @abstractmethod
    def debug(self, msg: str, *args, **kwargs):
        """
        Logs msg with severity 'DEBUG'.
        """

    @abstractmethod
    def info(self, msg: str, *args, **kwargs):
        """
        Logs msg with severity 'INFO'.
        """

    @abstractmethod
    def warning(self, msg: str, *args, **kwargs):
        """
        Logs msg with severity 'WARNING'.
        """

    @abstractmethod
    def error(self, msg: str, *args, **kwargs):
        """
        Logs msg with severity 'ERROR'.
        """

    @abstractmethod
    def exception(self, msg: str, *args, **kwargs):
        """
        Convenience method for logging an ERROR with exception information.
        """

    @abstractmethod
    def critical(self, msg: str, *args, **kwargs):
        """
        Logs msg with severity 'CRITICAL'.
        """

    @abstractmethod
    def log(self, msg: str, *args, **kwargs):
        """
        Logs msg with severity log level 100
        """
