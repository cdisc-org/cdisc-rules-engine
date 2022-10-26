from cdisc_rules_engine.config import config
from cdisc_rules_engine.services.logging import LoggingServiceFactory

logger = LoggingServiceFactory(config).get_service()
