import re
from copy import deepcopy
from typing import List

from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
)


class InMemoryCacheService(CacheServiceInterface):
    _instance = None

    @classmethod
    def get_instance(cls, **kwargs):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.cache = {}

    def add(self, cache_key, data):
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
        cache_contents: dict = deepcopy(self.cache)
        if prefix:
            for key, value in cache_contents.items():
                if key.startswith(prefix):
                    self.cache.pop(key)
        else:
            self.cache = {}

    def add_all(self, data: dict):
        self.cache = {**self.cache, **data}
