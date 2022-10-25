import logging
from typing import Type, Optional, Dict

from cdisc_rules_engine.constants import LOG_FORMAT
from cdisc_rules_engine.interfaces import (
    ConfigInterface,
    FactoryInterface,
    LoggerInterface,
)
from cdisc_rules_engine.services.logging import ConsoleLogger

logging.getLogger("asyncio").disabled = True
logging.getLogger("xmlschema").disabled = True
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)


class LoggingServiceFactory(FactoryInterface):
    _instance = None
    __registered_services: Dict[str, Type[LoggerInterface]] = {
        "console": ConsoleLogger,
    }

    def __init__(self, config: ConfigInterface):
        self._config = config
        self._default_service_name: str = (
            config.getValue("LOGGING_SERVICE_TYPE") or "console"
        )

    @classmethod
    def register_service(cls, name: str, service: Type[LoggerInterface]) -> None:
        if not issubclass(service, LoggerInterface):
            raise ValueError(f"Service must implement {LoggerInterface.__name__}")
        cls.__registered_services[name] = service

    def get_service(self, name: str = None, **kwargs) -> "LoggerInterface":
        service_name: str = name or self._default_service_name
        service_class: Optional[Type[LoggerInterface]] = self.__registered_services.get(
            service_name
        )
        if not service_class:
            raise ValueError(
                f"Service name must be in {list(self.__registered_services.keys())}, "
                f"given service name is {service_name}"
            )
        return service_class.get_instance(self._config)
