import logging
from engine.constants import LOG_FORMAT
from engine.config import ConfigService


class LoggingServiceFactory:
    _instance = None

    @classmethod
    def get_logger(cls, config: ConfigService):
        # TODO: Expand this function as more logging services are available are available.
        if cls._instance is None:
            logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
            logger = logging.getLogger()
            cls._instance = logger
        return cls._instance
