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
