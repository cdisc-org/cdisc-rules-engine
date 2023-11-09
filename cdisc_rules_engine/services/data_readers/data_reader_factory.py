from typing import Type

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
    FactoryInterface,
)
from cdisc_rules_engine.services.data_readers.xpt_reader import XPTReader
from cdisc_rules_engine.services.data_readers.json_reader import DatasetJSONReader
from cdisc_rules_engine.enums.dataformat_types import DataFormatTypes


class DataReaderFactory(FactoryInterface):
    _reader_map = {
        DataFormatTypes.XPT.value: XPTReader,
        DataFormatTypes.JSON.value: DatasetJSONReader,
    }

    def __init__(self, service_name: str = None):
        self._default_service_name = service_name

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
