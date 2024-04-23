from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import dask.dataframe as dd
import dask.array as da
import pandas as pd
from typing import List

DEFAULT_NUM_PARTITIONS = 4


class DaskDataset(PandasDataset):
    def __init__(
        self,
        data=dd.from_pandas(pd.DataFrame(), npartitions=DEFAULT_NUM_PARTITIONS),
        columns=None,
        length=None,
    ):
        self._data = data
        self.length = length
        if columns and self._data.empty:
            self._data = dd.from_pandas(
                pd.DataFrame(columns=columns), npartitions=DEFAULT_NUM_PARTITIONS
            )

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    def __getitem__(self, item):
        return self._data[item].compute().reset_index(drop=True)

    def __setitem__(self, key, value):
        if isinstance(value, list):
            chunks = self._data.map_partitions(lambda x: len(x)).compute().to_numpy()
            array_values = da.from_array(value, chunks=tuple(chunks))
            self._data[key] = array_values
        elif isinstance(value, dd.DataFrame):
            for column in value:
                self._data[column] = value[column]
        else:
            self._data[key] = value

    def __len__(self):
        return self.length or self._data.shape[0].compute()

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        dataframe = dd.from_dict(data, npartitions=DEFAULT_NUM_PARTITIONS, **kwargs)
        return cls(dataframe)

    @classmethod
    def from_records(cls, data: List[dict], **kwargs):
        data = pd.DataFrame.from_records(data)
        dataframe = dd.from_pandas(data, npartitions=DEFAULT_NUM_PARTITIONS)
        return cls(dataframe)

    def get(self, column: str, default=None):
        if column in self._data:
            return self._data[column].compute()
        return default

    def apply(self, func, **kwargs):
        return self._data.apply(func, **kwargs).compute()

    def merge(self, other, **kwargs):
        if isinstance(other, pd.Series):
            new_data = self._data.merge(
                dd.from_pandas(other.reset_index(), npartitions=self._data.npartitions),
                **kwargs
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

    def is_series(self, data) -> bool:
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

    @property
    def size(self):
        memory_usage = self.data.get_partition(0).compute().memory_usage()
        return memory_usage.sum()

    def assign(self, **kwargs):
        return self.data.assign(**kwargs)

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
                & self[column]
                .reset_index(drop=True)
                .eq(other_dataset[column].reset_index(drop=True))
                .all()
            )
        return is_equal

    def get_error_rows(self, results) -> "pd.Dataframe":
        """
        Returns a pandas dataframe with all errors found in the dataset. Limited to 1000
        """
        results_frame = results.to_frame()
        results_frame.columns = ["results"]
        data_with_results = self.data.merge(results_frame)
        return data_with_results[data_with_results["results"]].head(1000)

    @classmethod
    def cartesian_product(cls, left, right):
        """
        Return the cartesian product of two dataframes
        """
        return cls(
            dd.from_pandas(
                left.compute().merge(right, how="cross"),
                npartitions=DEFAULT_NUM_PARTITIONS,
            )
        )
