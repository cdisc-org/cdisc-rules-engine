import logging

from cdisc_rules_engine.interfaces import ConfigInterface, LoggerInterface


class ConsoleLogger(LoggerInterface):
    @classmethod
    def get_instance(cls, config: ConfigInterface):
        logger = logging.getLogger()
        return cls(logger, config)

    def __init__(self, logger, config: ConfigInterface):
        self._logger = logger
        self._config = config

    @property
    def disabled(self) -> bool:
        return self._logger.disabled

    @disabled.setter
    def disabled(self, value: bool):
        self._logger.disabled = value

    def setLevel(self, level: str):
        levels = {
            "info": logging.INFO,
            "debug": logging.DEBUG,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
            "warn": logging.WARNING,
            "verbose": logging.CRITICAL + 1,
        }
        self._logger.setLevel(levels.get(level, logging.ERROR))

    def debug(self, msg: str, *args, **kwargs):
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self._logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):
        self._logger.exception(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):
        self._logger.critical(msg, *args, **kwargs)

    def log(self, msg: str, *args, **kwargs):
        self._logger.log(logging.CRITICAL + 1, msg, *args, **kwargs)
