import pandas as pd
import pytest


class CacheService:
    def __init__(self):
        self._cache = {}

    def set(self, key, df):
        self._cache[key] = df

    def get_deepcopy(self, key):
        return self._cache[key].copy(deep=True)

    def get_cow(self, key):
        return self._cache[key]  # relying on CoW


@pytest.fixture
def sample_df():
    return pd.DataFrame({"A": [1, 2, 3], "B": [10, 20, 30]})


def test_deepcopy_does_not_modify_cache(sample_df):
    cache = CacheService()
    cache.set("x", sample_df)

    df = cache.get_deepcopy("x")
    df.loc[0, "A"] = 999

    cached = cache._cache["x"]

    assert cached.loc[0, "A"] == 1
    assert df.loc[0, "A"] == 999


def test_cow_does_not_modify_cache(sample_df):
    pd.options.mode.copy_on_write = True

    cache = CacheService()
    cache.set("x", sample_df)

    df = cache.get_cow("x")
    df.loc[0, "A"] = 999

    cached = cache._cache["x"]

    assert cached.loc[0, "A"] == 1
    assert df.loc[0, "A"] == 999


def test_cow_shares_memory_before_write(sample_df):
    pd.options.mode.copy_on_write = True

    cache = CacheService()
    cache.set("x", sample_df)

    df = cache.get_cow("x")

    assert df is cache._cache["x"]


def test_cow_inplace_operation(sample_df):
    pd.options.mode.copy_on_write = True

    cache = CacheService()
    cache.set("x", sample_df)

    df = cache.get_cow("x")

    df["A"].fillna(0, inplace=True)

    cached = cache._cache["x"]

    assert cached.equals(sample_df)


def test_cow_chained_assignment(sample_df):
    pd.options.mode.copy_on_write = True

    cache = CacheService()
    cache.set("x", sample_df)

    df = cache.get_cow("x")

    df_slice = df[df["A"] > 1]
    df_slice["A"] = 999  # chained assignment

    cached = cache._cache["x"]

    assert cached["A"].tolist() == [1, 2, 3]


def test_cow_numpy_view_can_break_isolation(sample_df):
    pd.options.mode.copy_on_write = True

    cache = CacheService()
    cache.set("x", sample_df)

    df = cache.get_cow("x")

    values = df["A"].values
    values[0] = 999  # bypassing pandas

    cached = cache._cache["x"]

    # CoW didn't work
    assert cached.loc[0, "A"] == 999


def test_cow_object_dtype_mutation():
    pd.options.mode.copy_on_write = True

    df = pd.DataFrame({"A": [[1], [2], [3]]})

    cache = CacheService()
    cache.set("x", df)

    df2 = cache.get_cow("x")

    df2.loc[0, "A"].append(999)  # mutate nested

    cached = cache._cache["x"]

    # cache changed
    assert cached.loc[0, "A"] == [1, 999]
