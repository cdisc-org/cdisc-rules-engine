from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.cache.redis_cache_service import RedisCacheService


class CacheServiceFactory:
    def __init__(self, config):
        self.config = config

    def get_cache_service(self):
        cache_service_type = self.config.getValue("CACHE_TYPE")
        if cache_service_type == "redis":
            return RedisCacheService.get_instance(self.config)
        else:
            return InMemoryCacheService.get_instance()
