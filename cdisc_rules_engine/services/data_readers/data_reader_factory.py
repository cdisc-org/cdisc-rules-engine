from typing import Type

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
    FactoryInterface,
)
from cdisc_rules_engine.services.data_readers.xpt_reader import XPTReader


class DataReaderFactory(FactoryInterface):
    _reader_map = {
        "xpt": XPTReader,
    }

    def __init__(self):
        self._default_service_name: str = "xpt"

    @classmethod
    def register_service(cls, name: str, service: Type[DataReaderInterface]):
        """
        Registers a new service in internal _service_map
        """
        if not name:
            raise ValueError("Service name must not be empty!")
        if not issubclass(service, DataReaderInterface):
            raise TypeError("Implementation of DataReaderInterface required!")
        cls._reader_map[name] = service

    def get_service(self, name: str = None, **kwargs) -> DataReaderInterface:
        """
        Get instance of service that matches searched implementation
        """
        service_name = name or self._default_service_name
        if service_name in self._reader_map:
            return self._reader_map[service_name]()
        raise ValueError(
            f"Service name must be in {list(self._reader_map.keys())}, "
            f"given service name is {service_name}"
        )
