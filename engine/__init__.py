from engine.config import config
from engine.services.data_service_factory import DataServiceFactory
from engine.services.cache.cache_service_factory import CacheServiceFactory


cache_service_obj = CacheServiceFactory(config).get_cache_service()
data_service_factory = DataServiceFactory(config, cache_service_obj)
