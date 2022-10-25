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
