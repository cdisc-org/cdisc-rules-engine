from abc import ABC, abstractmethod
from typing import Iterable, List


class CacheServiceInterface(ABC):
    @classmethod
    @abstractmethod
    def get_instance(cls, **kwargs) -> "CacheServiceInterface":
        pass

    def add(self, cache_key, data):
        raise NotImplementedError

    def add_batch(
        self,
        items: Iterable[dict],
        cache_key_name: str,
        pop_cache_key: bool = False,
        prefix: str = "",
    ):
        """
        Pushes a list of items to cache.
        cache_key_name param specifies the key whose
        value in each item will be used as cache key.
        If pop_cache_key is set to True, we need to delete
        item[cache_key_name] before saving to cache.
        prefix param specifies a prefix for the cache_key
        """
        raise NotImplementedError

    def get(self, cache_key):
        raise NotImplementedError

    def get_all(self, cache_keys: List[str]):
        raise NotImplementedError

    def get_all_by_prefix(self, prefix):
        raise NotImplementedError

    def exists(self, cache_key):
        raise NotImplementedError

    def clear(self, cache_key):
        raise NotImplementedError

    def clear_all(self, prefix: str = None):
        raise NotImplementedError

    def add_all(self, data: dict):
        raise NotImplementedError

    def filter_cache(self, prefix: str) -> dict:
        raise NotImplementedError

    def get_by_regex(self, regex: str) -> dict:
        raise NotImplementedError
