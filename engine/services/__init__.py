from engine.services.logging_service_factory import LoggingServiceFactory
from engine.config import config

logger = LoggingServiceFactory.get_logger(config)
