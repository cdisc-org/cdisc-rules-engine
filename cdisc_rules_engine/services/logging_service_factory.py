import logging

from cdisc_rules_engine.constants import LOG_FORMAT
from cdisc_rules_engine.interfaces import ConfigInterface

logging.getLogger("asyncio").disabled = True
logging.getLogger("xmlschema").disabled = True

logging.getLogger("asyncio").disabled = True
logging.getLogger("xmlschema").disabled = True


class LoggingServiceFactory:
    _instance = None

    @classmethod
    def get_logger(cls, config: ConfigInterface):
        # TODO: Expand this function as more logging services are available.
        if cls._instance is None:
            logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
            logger = logging.getLogger()
            cls._instance = logger
        return cls._instance
