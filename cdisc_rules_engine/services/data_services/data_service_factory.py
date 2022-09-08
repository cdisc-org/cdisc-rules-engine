from typing import List, Type

from cdisc_rules_engine.config import ConfigService
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
    DataServiceInterface,
    FactoryInterface,
)

from . import DummyDataService, LocalDataService


class DataServiceFactory(FactoryInterface):
    _registered_services_map = {"local": LocalDataService, "dummy": DummyDataService}

    def __init__(self, config: ConfigService, cache_service: CacheServiceInterface):
        self.data_service_name = config.getValue("DATA_SERVICE_TYPE") or "local"
        self.config = config
        self.cache_service = cache_service

    def get_data_service(self) -> DataServiceInterface:
        """Get local data service"""
        return self.get_service("local")

    def get_dummy_data_service(self, data: List[DummyDataset]) -> DataServiceInterface:
        return self.get_service("dummy", data=data)

    @classmethod
    def register_service(cls, name: str, service: Type[DataServiceInterface]) -> None:
        """
        Save mapping of service name and it's implementation
        """
        if not name:
            raise ValueError("Service name must not be empty!")
        if not issubclass(service, DataServiceInterface):
            raise TypeError("Implementation of DataServiceInterface required!")
        cls._registered_services_map[name] = service

    def get_service(self, name: str = None, **kwargs) -> DataServiceInterface:
        """Get instance of service that matches searched implementation"""
        service_name = name or self.data_service_name
        if service_name in self._registered_services_map:
            return self._registered_services_map.get(service_name).get_instance(
                config=self.config, cache_service=self.cache_service, **kwargs
            )
        raise ValueError(
            f"Service name must be in  {list(self._registered_services_map.keys())}, "
            f"given service name is {service_name}"
        )
