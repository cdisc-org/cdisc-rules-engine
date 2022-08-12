from cdisc_rules_engine.config import config
from cdisc_rules_engine.services.logging_service_factory import LoggingServiceFactory

logger = LoggingServiceFactory.get_logger(config)
