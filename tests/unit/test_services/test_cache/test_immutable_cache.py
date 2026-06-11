import numpy as np
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


class TestGet:
    def test_returns_new_wrapper_not_cached_object(self, cache, sample_dataset):
        cache.add("x", sample_dataset)
        result = cache.get("x")
        assert result is not cache.cache["x"]
        assert result.data is not cache.cache["x"].data

    def test_cow_does_not_modify_cache_on_write(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("x", sample_dataset)
        retrieved = cache.get("x")
        retrieved.data.loc[0, "A"] = 999
        assert cache.cache["x"].data.loc[0, "A"] == 1

    def test_shares_memory_before_write(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("x", sample_dataset)
        retrieved = cache.get("x")
        assert np.shares_memory(retrieved.data["A"], cache.cache["x"].data["A"])

    def test_add_rows_does_not_affect_cache(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("x", sample_dataset)
        retrieved = cache.get("x")
        retrieved.data = pd.concat(
            [retrieved.data, pd.DataFrame({"A": [999], "B": [999]})],
            ignore_index=True,
        )
        assert len(cache.cache["x"].data) == 3
        assert len(retrieved.data) == 4

    def test_drop_rows_does_not_affect_cache(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("x", sample_dataset)
        retrieved = cache.get("x")
        retrieved.data = retrieved.data.drop(index=0).reset_index(drop=True)
        assert len(cache.cache["x"].data) == 3
        assert len(retrieved.data) == 2

    def test_filter_rows_does_not_affect_cache(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("x", sample_dataset)
        retrieved = cache.get("x")
        retrieved.data = retrieved.data[retrieved.data["A"] > 1].reset_index(drop=True)
        assert len(cache.cache["x"].data) == 3
        assert cache.cache["x"].data["A"].tolist() == [1, 2, 3]

    def test_multiple_gets_are_independent(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("x", sample_dataset)
        first = cache.get("x")
        second = cache.get("x")
        first.data = first.data.drop(index=0).reset_index(drop=True)
        assert len(second.data) == 3
        assert len(cache.cache["x"].data) == 3

    def test_non_dataset_returns_as_is(self, cache):
        cache.add("key", {"some": "dict"})
        assert cache.get("key") == {"some": "dict"}

    def test_object_dtype_nested_mutation_affects_cache(self, cache):
        """CoW can't protect in nested mutations"""
        df = pd.DataFrame({"A": [[1], [2], [3]]})
        cache.add("x", PandasDataset(df))
        retrieved = cache.get("x")
        retrieved.data.loc[0, "A"].append(999)
        assert cache.cache["x"].data.loc[0, "A"] == [1, 999]


class TestGetDataset:
    def test_returns_new_wrapper_not_cached_object(self, cache, sample_dataset):
        cache.add_dataset("x", sample_dataset)
        result = cache.get_dataset("x")
        assert result is not cache.dataset_cache["x"]
        assert result.data is not cache.dataset_cache["x"].data

    def test_cow_does_not_modify_cache_on_write(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add_dataset("x", sample_dataset)
        retrieved = cache.get_dataset("x")
        retrieved.data.loc[0, "A"] = 999
        assert cache.dataset_cache["x"].data.loc[0, "A"] == 1

    def test_add_rows_does_not_affect_cache(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add_dataset("x", sample_dataset)
        retrieved = cache.get_dataset("x")
        retrieved.data = pd.concat(
            [retrieved.data, pd.DataFrame({"A": [999], "B": [999]})],
            ignore_index=True,
        )
        assert len(cache.dataset_cache["x"].data) == 3
        assert len(retrieved.data) == 4

    def test_drop_rows_does_not_affect_cache(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add_dataset("x", sample_dataset)
        retrieved = cache.get_dataset("x")
        retrieved.data = retrieved.data.drop(index=0).reset_index(drop=True)
        assert len(cache.dataset_cache["x"].data) == 3
        assert len(retrieved.data) == 2


class TestGetAll:
    def test_returns_new_wrappers(self, cache, sample_dataset):
        cache.add("x", sample_dataset)
        cache.add("y", sample_dataset)
        results = cache.get_all(["x", "y"])
        assert all(r is not cache.cache["x"] for r in results)
        assert all(r.data is not cache.cache["x"].data for r in results)

    def test_results_are_independent(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("x", sample_dataset)
        cache.add("y", sample_dataset)
        first, second = cache.get_all(["x", "y"])
        first.data = first.data.drop(index=0).reset_index(drop=True)
        assert len(second.data) == 3
        assert len(cache.cache["x"].data) == 3

    def test_cow_does_not_modify_cache_on_write(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("x", sample_dataset)
        results = cache.get_all(["x"])
        results[0].data.loc[0, "A"] = 999
        assert cache.cache["x"].data.loc[0, "A"] == 1

    def test_missing_key_returns_none(self, cache):
        assert cache.get_all(["missing"]) == [None]


class TestGetAllByPrefix:
    def test_returns_only_matching_keys(self, cache, sample_dataset):
        cache.add("ds/ae", sample_dataset)
        cache.add("ds/lb", sample_dataset)
        cache.add("other/ae", sample_dataset)
        results = cache.get_all_by_prefix("ds/")
        assert len(results) == 2

    def test_returns_new_wrappers(self, cache, sample_dataset):
        cache.add("ds/ae", sample_dataset)
        results = cache.get_all_by_prefix("ds/")
        assert results[0] is not cache.cache["ds/ae"]
        assert results[0].data is not cache.cache["ds/ae"].data

    def test_cow_does_not_modify_cache_on_write(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("ds/ae", sample_dataset)
        results = cache.get_all_by_prefix("ds/")
        results[0].data.loc[0, "A"] = 999
        assert cache.cache["ds/ae"].data.loc[0, "A"] == 1

    def test_drop_rows_does_not_affect_cache(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("ds/ae", sample_dataset)
        results = cache.get_all_by_prefix("ds/")
        results[0].data = results[0].data.drop(index=0).reset_index(drop=True)
        assert len(cache.cache["ds/ae"].data) == 3

    def test_no_match_returns_empty(self, cache, sample_dataset):
        cache.add("ds/ae", sample_dataset)
        assert cache.get_all_by_prefix("other/") == []


class TestGetByRegex:
    def test_returns_matching_keys(self, cache, sample_dataset):
        cache.add("ae_data", sample_dataset)
        cache.add("lb_data", sample_dataset)
        cache.add("ae_meta", sample_dataset)
        result = cache.get_by_regex("ae_*")
        assert set(result.keys()) == {"ae_data", "ae_meta"}

    def test_returns_new_wrappers(self, cache, sample_dataset):
        cache.add("ae_data", sample_dataset)
        result = cache.get_by_regex("ae_*")
        assert result["ae_data"] is not cache.cache["ae_data"]
        assert result["ae_data"].data is not cache.cache["ae_data"].data

    def test_cow_does_not_modify_cache_on_write(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("ae_data", sample_dataset)
        result = cache.get_by_regex("ae_*")
        result["ae_data"].data.loc[0, "A"] = 999
        assert cache.cache["ae_data"].data.loc[0, "A"] == 1

    def test_drop_rows_does_not_affect_cache(self, cache, sample_dataset):
        pd.options.mode.copy_on_write = True
        cache.add("ae_data", sample_dataset)
        result = cache.get_by_regex("ae_*")
        result["ae_data"].data = (
            result["ae_data"].data.drop(index=0).reset_index(drop=True)
        )
        assert len(cache.cache["ae_data"].data) == 3

    def test_no_match_returns_empty_dict(self, cache, sample_dataset):
        cache.add("ae_data", sample_dataset)
        assert cache.get_by_regex("lb_*") == {}
