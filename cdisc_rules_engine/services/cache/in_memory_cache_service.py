import re
from typing import List
from pympler import asizeof
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
)
from cachetools import LRUCache
import psutil


class InMemoryCacheService(CacheServiceInterface):
    _instance = None

    @classmethod
    def get_instance(cls, **kwargs):
        if cls._instance is None:
            cls._instance = cls(**kwargs)
        return cls._instance

    def __init__(self, max_size=None, **kwargs):
        self.max_size = max_size or psutil.virtual_memory().available * 0.75
        self.cache = LRUCache(maxsize=self.max_size, getsizeof=asizeof.asizeof)

    def add(self, cache_key, data):
        if asizeof.asizeof(data) > self.max_size:
            return
        self.cache[cache_key] = data

    def add_batch(
        self,
        items: List[dict],
        cache_key_name: str,
        pop_cache_key: bool = False,
        prefix: str = "",
    ):
        for item in items:
            cache_key: str = item[cache_key_name]
            if pop_cache_key:
                item.pop(cache_key_name)
            self.add(prefix + cache_key, item)

    def get(self, cache_key):
        return self.cache.get(cache_key, None)

    def get_all(self, cache_keys: List[str]):
        return [self.cache.get(key) for key in cache_keys]

    def get_all_by_prefix(self, prefix):
        items = []
        for key in self.cache:
            if key.startswith(prefix):
                items.append(self.cache[key])
        return items

    def filter_cache(self, prefix: str) -> dict:
        return {k: self.cache[k] for k in self.cache.keys() if k.startswith(prefix)}

    def get_by_regex(self, regex: str) -> dict:
        regex = regex.replace("*", ".*")
        return {k: self.cache[k] for k in self.cache.keys() if re.search(regex, k)}

    def exists(self, cache_key):
        return cache_key in self.cache

    def clear(self, cache_key):
        self.cache.pop(cache_key, "invalid")

    def clear_all(self, prefix: str = None):
        if prefix:
            keys_to_remove = [
                key for key in self.cache.keys() if key.startswith(prefix)
            ]
            for key in keys_to_remove:
                self.clear(key)
        else:
            self.cache = LRUCache(maxsize=self.max_size, getsizeof=asizeof.asizeof)

    def add_all(self, data: dict):
        for key, val in data.items():
            self.add(key, val)
