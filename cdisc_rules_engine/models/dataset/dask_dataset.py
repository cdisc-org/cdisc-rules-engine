from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import dask.dataframe as dd
import dask.array as da
import pandas as pd
import numpy as np
import re
import dask
from typing import List, Union

DEFAULT_NUM_PARTITIONS = 4
dask.config.set({"dataframe.convert-string": False})


class DaskDataset(PandasDataset):
    def __init__(
        self,
        data=dd.from_pandas(pd.DataFrame(), npartitions=DEFAULT_NUM_PARTITIONS),
        columns=None,
        length=None,
    ):
        if isinstance(data, pd.DataFrame):
            data = dd.from_pandas(data, npartitions=DEFAULT_NUM_PARTITIONS)
        self._data = data
        self.length = length
        if columns and self._data.empty:
            self._data = dd.from_pandas(
                pd.DataFrame(columns=columns), npartitions=DEFAULT_NUM_PARTITIONS
            )

    @property
    def data(self):
        return self._data

    @property
    def loc(self):
        return self._data.loc

    @property
    def size(self):
        memory_usage = self.data.get_partition(0).compute().memory_usage()
        return memory_usage.sum()

    @data.setter
    def data(self, data):
        self._data = data

    def __getitem__(self, item):
        try:
            if hasattr(item, "dtype") and pd.api.types.is_bool_dtype(item.dtype):
                # Handle boolean indexing
                return self._data.compute()[item]
            return self._data[item].compute().reset_index(drop=True)
        except ValueError as e:
            # Handle boolean indexing length mismatch which occurs when filtering
            # empty DataFrames or when metadata doesn't match actual data dimensions
            if "Item wrong length" in str(e):
                empty_df = pd.DataFrame(columns=self._data.columns)
                return empty_df
            raise

    def is_column_sorted_within(self, group, column):
        return (
            False
            not in np.concatenate(
                self._data.groupby(group, sort=False)[column]
                .apply(
                    lambda partition: sorted(partition.sort_index().values)
                    == partition.sort_index().values
                )
                .compute()
                .values
            )
            .ravel()
            .tolist()
        )

    def __setitem__(self, key, value):
        if isinstance(value, list):
            chunks = self._data.map_partitions(lambda x: len(x)).compute().to_numpy()
            array_values = da.from_array(value, chunks=tuple(chunks))
            self._data[key] = array_values
        elif isinstance(value, pd.Series):
            self._data = self._data.reset_index()
            self._data = self._data.set_index("index")
            self._data[key] = value
        elif isinstance(value, dd.DataFrame):
            for column in value:
                self._data[column] = value[column]
        else:
            self._data[key] = value

    def __len__(self):
        if not self.length:
            length = self._data.shape[0]
            if not isinstance(length, int):
                length = length.compute()
            self.length = length

        return self.length

    def __deepcopy__(self, memo):
        pandas_df = self._data.compute()
        fresh_dask_df = dd.from_pandas(pandas_df, npartitions=DEFAULT_NUM_PARTITIONS)
        new_instance = self.__class__(fresh_dask_df)
        new_instance.length = self.length
        memo[id(self)] = new_instance
        return new_instance

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        dataframe = dd.from_dict(data, npartitions=DEFAULT_NUM_PARTITIONS, **kwargs)
        return cls(dataframe)

    @classmethod
    def from_records(cls, data: List[dict], **kwargs):
        data = pd.DataFrame.from_records(data, **kwargs)
        dataframe = dd.from_pandas(data, npartitions=DEFAULT_NUM_PARTITIONS)
        return cls(dataframe)

    @classmethod
    def get_series_values(cls, series) -> list:
        if not cls.is_series(series):
            return []
        if isinstance(cls, pd.Series):
            return series.values
        else:
            return series.compute().values

    def get(self, target: Union[str, List[str]], default=None):
        if isinstance(target, list):
            for column in target:
                if column not in self._data:
                    # List contains values not in the dataset, treat as list of values
                    return default
            return self._data[target].compute()
        elif target in self._data:
            return self._data[target].compute()
        return default

    def convert_to_series(self, result):
        if self.is_series(result):
            if isinstance(result, dd.Series):
                result = result.compute()
            if pd.api.types.is_bool_dtype(result.dtype):
                return result.astype("bool")
            return result
        series = pd.Series(result)
        if pd.api.types.is_bool_dtype(series.dtype):
            return series.astype("bool")
        return series

    def apply(self, func, **kwargs):
        return self._data.apply(func, **kwargs).compute()

    def merge(self, other, **kwargs):
        if isinstance(other, pd.Series):
            new_data = self._data.merge(
                dd.from_pandas(other.reset_index(), npartitions=self._data.npartitions),
                **kwargs,
            )
        else:
            new_data = self._data.merge(other, **kwargs)
        return self.__class__(new_data)

    def __concat_columns(self, current, other):
        for column in other.columns:
            current[column] = other[column]
        return current

    def concat(self, other, **kwargs):
        if kwargs.get("axis", 0) == 1:
            if isinstance(other, list):
                new_data = self._data.copy()
                for dataset in other:
                    new_data = self.__concat_columns(new_data, dataset)
            else:
                new_data = self.__concat_columns(self._data.copy(), other)
        else:
            if isinstance(other, list):
                datasets = [dataset.data for dataset in other]
                new_data = dd.concat([self._data] + datasets, **kwargs)
            else:
                new_data = dd.concat([self._data.copy(), other.data], **kwargs)

        return self.__class__(new_data)

    def groupby(self, by: List[str], **kwargs):
        invalid_kwargs = ["as_index"]
        return self.__class__(
            self._data.groupby(
                by, **self._remove_invalid_kwargs(invalid_kwargs, kwargs)
            )
        )

    def get_grouped_size(self, by, **kwargs):
        if isinstance(self._data, pd.DataFrame):
            grouped_data = self._data[by].groupby(by, **kwargs)
        else:
            grouped_data = self._data[by].compute().groupby(by, **kwargs)
        group_sizes = grouped_data.size()
        if self.is_series(group_sizes):
            group_sizes = group_sizes.to_frame(name="size")

        return group_sizes

    @classmethod
    def is_series(cls, data) -> bool:
        return isinstance(data, dd.Series) or isinstance(data, pd.Series)

    def len(self) -> int:
        return self._data.shape[0].compute()

    def rename(self, index=None, columns=None, inplace=True):
        self._data = self._data.rename(index=index, columns=columns)
        return self

    def drop(self, labels=None, axis=0, columns=None, errors="raise"):
        """
        Drop specified labels from rows or columns.
        """
        self._data = self._data.drop(
            labels=labels, axis=axis, columns=columns, errors=errors
        )
        return self

    def melt(
        self,
        id_vars=None,
        value_vars=None,
        var_name=None,
        value_name="value",
        col_level=None,
    ):
        """
        Unpivots a DataFrame from wide format to long format,
        optionally leaving identifier variables set.
        """
        new_data = self._data.melt(
            id_vars=id_vars,
            var_name=var_name,
            value_vars=value_vars,
            value_name=value_name,
            col_level=col_level,
        )
        return self.__class__(new_data)

    def assign(self, **kwargs):
        return self.__class__(self.data.assign(**kwargs))

    def copy(self):
        new_data = self._data.copy()
        return self.__class__(new_data)

    def equals(self, other_dataset):
        is_equal = True
        for column in self.data:
            if column not in other_dataset:
                return False
            is_equal = (
                is_equal
                and self[column]
                .reset_index(drop=True)
                .eq(other_dataset[column].reset_index(drop=True))
                .all()
            )
        return is_equal

    def get_error_rows(self, results) -> "pd.Dataframe":
        """
        Returns a pandas dataframe with all errors found in the dataset. Limited to 1000
        """
        self.data["computed_index"] = 1
        self.data["computed_index"] = self.data["computed_index"].cumsum() - 1
        data_with_results = self.data.set_index("computed_index", sorted=True)
        data_with_results["results"] = results
        data_with_results = data_with_results.fillna(value={"results": False})
        return data_with_results[data_with_results["results"]].head(
            1000, npartitions=-1
        )

    def dropna(self, inplace=False, **kwargs):
        result = self._data.dropna(**kwargs)
        if inplace:
            self._data = result
            return None
        else:
            return self.__class__(result)

    def at(self, row_label, col_label):
        """
        Get a single value for a row/column pair.
        """
        partition_index = self.data.loc[row_label:row_label].partitions[0].compute()
        value = partition_index.at[row_label, col_label]
        return value

    def drop_duplicates(self, subset=None, keep="first", **kwargs):
        """
        Drop duplicate rows from the dataset.
        """
        new_data = self._data.drop_duplicates(subset=subset, keep=keep, **kwargs)
        return self.__class__(new_data)

    def replace(self, to_replace, value, **kwargs):
        self._data = self._data.replace(to_replace, value, **kwargs)
        return self

    def astype(self, dtype, **kwargs):
        self._data = self._data.astype(dtype, **kwargs)
        return self

    def filter(self, **kwargs):
        columns_regex = kwargs.get("regex")
        columns_subset = [
            column for column in self.columns if re.match(columns_regex, column)
        ]
        new_data = self._data[columns_subset]
        return self.__class__(new_data)

    def min(self, *args, **kwargs):
        """
        Return the minimum of the values over the requested axis.
        """
        result = self._data.min(*args, **kwargs)
        return self.__class__(result)

    def reset_index(self, drop=False, **kwargs):
        """
        Reset the index of the dataset.
        """
        self._data = self._data.reset_index(drop=drop, **kwargs)
        return self

    def iloc(self, n=None, column=None):
        """
        Purely integer-location based indexing for selection by position.
        """
        if column is None:
            return self._data.iloc[n].compute()
        else:
            return self._data.iloc[n, column].compute()

    def fillna(
        self,
        value=None,
        method=None,
        axis=None,
        inplace=False,
        limit=None,
        downcast=None,
    ):
        """
        Fill NA/NaN values using the specified method.
        """
        result = self._data.fillna(value=value, method=method, axis=axis, limit=limit)
        if inplace:
            self._data = result
            return None
        else:
            return self.__class__(result)

    def to_dict(self, **kwargs) -> dict:
        orient = kwargs.get("orient", "dict")
        if orient == "records":
            reset_df = self._data.reset_index(drop=True)
            all_partitions = list(
                reset_df.map_partitions(lambda x: x.to_dict(orient="records"))
            )
            flattened = []
            for partition in all_partitions:
                flattened.extend(partition)
            return flattened
        else:
            return self._data.compute().to_dict(**kwargs)

    def items(self, **kwargs):
        computed_df = self._data.compute()
        return computed_df.to_dict(**kwargs).items()

    def keys(self, **kwargs):
        """
        Returns a object containing the keys in the dataset dictionary.
        """
        computed_df = self._data.compute()
        return computed_df.to_dict(**kwargs).keys()

    def values(self, **kwargs):
        """
        Returns a object containing the values in the dataset dictionary.
        """
        computed_df = self._data.compute()
        return computed_df.to_dict(**kwargs).values()

    def isin(self, values):
        values_set = set(values)

        def partition_isin(partition):
            return partition.isin(values_set)

        result = self._data.map_partitions(partition_isin)
        return result

    def filter_by_value(self, column, values):
        mask = self._data[column].isin(values)
        return self.__class__(self._data[mask])

    def max(self, *args, **kwargs):
        result = self._data.max(*args, **kwargs)
        return self.__class__(result)
