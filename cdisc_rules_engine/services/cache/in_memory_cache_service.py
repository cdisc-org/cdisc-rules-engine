import re
from typing import List
from pympler import asizeof
from cdisc_rules_engine.interfaces import (
    CacheServiceInterface,
)
from cdisc_rules_engine.models.dataset import DatasetInterface
from cachetools import LRUCache
import psutil


def get_data_size(dataset):
    if isinstance(dataset, DatasetInterface):
        return dataset.size
    else:
        return asizeof.asizeof(dataset)


class InMemoryCacheService(CacheServiceInterface):
    _instance = None

    @classmethod
    def get_instance(cls, **kwargs):
        if cls._instance is None:
            cls._instance = cls(**kwargs)
        return cls._instance

    def __init__(self, max_size=None, **kwargs):
        self.max_size = max_size or psutil.virtual_memory().available * 0.25
        self.cache = LRUCache(maxsize=self.max_size, getsizeof=asizeof.asizeof)
        self.max_dataset_cache_size = psutil.virtual_memory().available * 0.5
        self.dataset_cache = LRUCache(
            maxsize=self.max_dataset_cache_size, getsizeof=get_data_size
        )

    def add(self, cache_key, data):
        if get_data_size(data) > self.max_size:
            return
        self.cache[cache_key] = data

    def add_dataset(self, cache_key, data):
        self.dataset_cache[cache_key] = data

    def get_dataset(self, cache_key):
        return self.dataset_cache.get(cache_key, None)

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

    def dataset_keys(self):
        return self.dataset_cache.keys()

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
