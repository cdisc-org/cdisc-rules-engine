import logging

import pytest

from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)


def test_cache_key_exceeds_max_size():
    cache = InMemoryCacheService(max_size=56)
    cache.add("key" * 10000, "test data")

    assert cache.exists("key" * 10000)


@pytest.mark.skip(reason="Demo test to show value too large scenario")
def test_demo_raises_when_data_becomes_bigger_after_check(monkeypatch, caplog):
    cache = InMemoryCacheService(max_size=56)
    original_setitem = cache.cache.__setitem__

    def exploding_setitem(self, key, value):
        value = value + value
        original_setitem(key, value)

    monkeypatch.setattr(
        cache.cache.__class__,
        "__setitem__",
        exploding_setitem,
    )
    with caplog.at_level(logging.WARNING):
        cache.add("test", "test data")

    assert "Failed to add result to cache for key 'test'" in caplog.text
    assert "value too large" in caplog.text
    assert "test" not in cache.cache


def test_cache_value_exceeds_max_size():
    cache = InMemoryCacheService(max_size=1)
    cache.add("test", "this is a test")
    assert cache.get("test") is None


def test_get_all_by_prefix():
    cache = InMemoryCacheService()
    cache.add("test", "this is a test")
    assert cache.get_all_by_prefix("te") == ["this is a test"]


def test_clear():
    cache = InMemoryCacheService()
    cache.add("test", "this is a test")
    cache.clear("test")
    assert cache.get("test") is None


def test_filter_cache():
    cache = InMemoryCacheService()
    cache.add("test", "this is a test")
    assert cache.filter_cache("te") == {"test": "this is a test"}


def test_get_by_regex():
    cache = InMemoryCacheService()
    cache.add("test", "this is a test")
    cache.add("hi", "bye")
    assert cache.get_by_regex("*t") == {"test": "this is a test"}


def test_clear_all_with_prefix():
    cache = InMemoryCacheService()
    cache.add("test", "this is a test")
    cache.add("hi", "bye")
    cache.clear_all("te")
    assert cache.exists("hi")
    assert not cache.exists("test")
