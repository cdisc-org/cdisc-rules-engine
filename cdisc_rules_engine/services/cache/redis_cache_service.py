import pickle
from typing import List

import redis

from cdisc_rules_engine.config import ConfigService
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
)


class RedisCacheService(CacheServiceInterface):
    _instance = None

    @classmethod
    def get_instance(cls, config: ConfigService, **kwargs):
        if cls._instance is None:
            instance = cls(
                config.getValue("REDIS_HOST_NAME"), config.getValue("REDIS_ACCESS_KEY")
            )
            cls._instance = instance
        return cls._instance

    def __init__(self, host_name, access_key):
        self.client = redis.Redis(
            host=host_name, port=6380, db=0, password=access_key, ssl=True
        )

    def add(self, cache_key, data):
        data = pickle.dumps(data)
        return self.client.set(cache_key, data)

    def add_batch(
        self,
        items: List[dict],
        cache_key_name: str,
        pop_cache_key: bool = False,
        prefix: str = "",
    ):
        logger.info(
            f"Saving batch to Redis cache. items={items},"
            f" cache_key_name={cache_key_name}"
        )
        with self.client.pipeline() as pipe:
            for item in items:
                cache_key: str = item[cache_key_name]
                if pop_cache_key:
                    item.pop(cache_key_name)
                pipe.set(prefix + cache_key, pickle.dumps(item))
            response: list = pipe.execute()
        logger.info(
            f"Successfully saved batch to Redis cache. Redis response = {response}"
        )

    def get(self, cache_key):
        cached_data = self.client.get(cache_key)
        if cached_data:
            return pickle.loads(cached_data)
        else:
            return None

    def get_all_by_prefix(self, prefix):
        keys = [key for key in self.client.scan_iter(match=f"{prefix}*")]
        return [pickle.loads(cached_data) for cached_data in self.client.mget(keys)]

    def exists(self, cache_key):
        return self.client.exists(cache_key)

    def clear(self, cache_key):
        return self.client.delete(cache_key)

    def clear_all(self, prefix: str = None):
        if prefix:
            prefix = f"{prefix}*"
        logger.info(f"Deleting all items with prefix = {prefix}")
        for key in self.client.scan_iter(prefix):
            self.client.delete(key)

    def filter_cache(self, prefix: str) -> dict:
        keys = [key for key in self.client.scan_iter(match=f"{prefix}*")]
        key_value_pairs = zip(keys, self.client.mget(keys))
        return {key: pickle.loads(value) for key, value in key_value_pairs}

    def get_by_regex(self, regex: str) -> dict:
        keys = [key for key in self.client.scan_iter(match=f"{regex}")]
        key_value_pairs = zip(keys, self.client.mget(keys))
        return {key: pickle.loads(value) for key, value in key_value_pairs}

    def add_all(self, data: dict):
        raise NotImplementedError("Method add_all not implemented in RedisCacheService")
