from typing import Type

from cdisc_rules_engine.config import ConfigService
from . import DummyDataService, BaseDataService, LocalDataService
from ..cache import CacheServiceInterface
from ..interfaces import FactoryInterface


class DataServiceFactory(FactoryInterface):
    _service_map = {"local": LocalDataService, "dummy": DummyDataService}

    def __init__(self, config: ConfigService, cache_service: CacheServiceInterface):
        self.config = config
        self.cache_service = cache_service

    def get_data_service(self) -> BaseDataService:
        """Get local data service"""
        return self.get_service("local", cache_service=self.cache_service)

    @classmethod
    def get_dummy_data_service(cls, data) -> BaseDataService:
        return cls.get_service("dummy", data=data)

    @classmethod
    def register_service(cls, name: str, service: Type[BaseDataService]) -> None:
        """
        Save mapping of service name and it's implementation
        """
        if issubclass(service, BaseDataService):
            cls._service_map[name] = service
            return
        raise TypeError("Implementation of BaseDataService required!")

    @classmethod
    def get_service(cls, name: str, **kwargs) -> BaseDataService:
        """Get class that matches searched implementation"""
        if name and name in cls._service_map.keys():
            return cls._service_map.get(name)(**kwargs)
        raise ValueError(f"No registered service named {name}")
