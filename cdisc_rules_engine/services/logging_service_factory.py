import logging

from cdisc_rules_engine.config import ConfigService
from cdisc_rules_engine.constants import LOG_FORMAT

logging.getLogger("asyncio").disabled = True
logging.getLogger("xmlschema").disabled = True


class LoggingServiceFactory:
    _instance = None

    @classmethod
    def get_logger(cls, config: ConfigService):
        # TODO: Expand this function as more logging services are available.
        if cls._instance is None:
            logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
            logger = logging.getLogger()
            cls._instance = logger
        return cls._instance
