import pandas as pd
import pytest

from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)


@pytest.fixture(autouse=True)
def reset_singleton():
    InMemoryCacheService._instance = None
    yield
    InMemoryCacheService._instance = None


@pytest.fixture
def cache():
    return InMemoryCacheService()


@pytest.fixture
def sample_dataset():
    return PandasDataset(pd.DataFrame({"A": [1, 2, 3], "B": [10, 20, 30]}))


def test_get_returns_cow_copy(cache, sample_dataset):
    cache.add("x", sample_dataset)
    result = cache.get("x")
    assert result is not sample_dataset
    assert result.data is not sample_dataset.data


def test_get_cow_does_not_modify_cache_on_write(cache, sample_dataset):
    pd.options.mode.copy_on_write = True
    cache.add("x", sample_dataset)

    retrieved = cache.get("x")
    retrieved.data.loc[0, "A"] = 999

    cached_data = cache.cache["x"].data
    assert cached_data.loc[0, "A"] == 1


def test_get_cow_shares_memory_before_write(cache, sample_dataset):
    pd.options.mode.copy_on_write = True
    cache.add("x", sample_dataset)
    retrieved = cache.get("x")
    import numpy as np

    assert np.shares_memory(retrieved.data["A"], cache.cache["x"].data["A"])


def test_get_dataset_returns_cow_copy(cache, sample_dataset):
    cache.add_dataset("x", sample_dataset)
    result = cache.get_dataset("x")
    assert result is not sample_dataset
    assert result.data is not sample_dataset.data


def test_get_dataset_cow_does_not_modify_cache_on_write(cache, sample_dataset):
    pd.options.mode.copy_on_write = True
    cache.add_dataset("x", sample_dataset)

    retrieved = cache.get_dataset("x")
    retrieved.data.loc[0, "A"] = 999

    cached_data = cache.dataset_cache["x"].data
    assert cached_data.loc[0, "A"] == 1


def test_get_object_dtype_nested_mutation_affects_cache(cache):
    """CoW can't protect in nested mutations"""
    df = pd.DataFrame({"A": [[1], [2], [3]]})
    dataset = PandasDataset(df)
    cache.add("x", dataset)

    retrieved = cache.get("x")
    retrieved.data.loc[0, "A"].append(999)

    cached_data = cache.cache["x"].data
    assert cached_data.loc[0, "A"] == [1, 999]


def test_get_non_dataset_returns_as_is(cache):
    cache.add("key", {"some": "dict"})
    result = cache.get("key")
    assert result == {"some": "dict"}


def test_get_returns_new_wrapper_not_cached_object(cache, sample_dataset):
    """get() должен возвращать новый PandasDataset, а не сам объект из кэша."""
    cache.add("x", sample_dataset)
    result = cache.get("x")
    assert result is not cache.cache["x"]  # новый wrapper
    assert (
        result.data is not cache.cache["x"].data
    )  # новый pd.DataFrame объект (shallow copy)


def test_get_dataset_returns_new_wrapper_not_cached_object(cache, sample_dataset):
    cache.add_dataset("x", sample_dataset)
    result = cache.get_dataset("x")
    assert result is not cache.dataset_cache["x"]
    assert result.data is not cache.dataset_cache["x"].data
