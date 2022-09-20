from typing import Type

from cdisc_rules_engine.interfaces import CacheServiceInterface, FactoryInterface

from .in_memory_cache_service import InMemoryCacheService
from .redis_cache_service import RedisCacheService


class CacheServiceFactory(FactoryInterface):
    _registered_services_map = {
        "redis": RedisCacheService,
        "in_memory": InMemoryCacheService,
    }

    def __init__(self, config):
        self.config = config
        self.cache_service_name = self.config.getValue("CACHE_TYPE") or "in_memory"

    def get_cache_service(self):
        return self.get_service()

    @classmethod
    def register_service(cls, name: str, service: Type[CacheServiceInterface]):
        if not name:
            raise ValueError("Service name must not be empty!")
        if not issubclass(service, CacheServiceInterface):
            raise TypeError("Implementation of CacheServiceInterface required!")
        cls._registered_services_map[name] = service

    def get_service(self, name: str = None, **kwargs) -> CacheServiceInterface:
        service_name = name or self.cache_service_name
        if service_name in self._registered_services_map:
            return self._registered_services_map.get(service_name).get_instance(
                config=self.config, **kwargs
            )
        raise ValueError(
            f"Service name must be in" f"  {list(self._registered_services_map.keys())}"
        )
