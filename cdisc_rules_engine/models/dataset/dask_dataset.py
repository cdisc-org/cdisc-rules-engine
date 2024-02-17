from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import dask.dataframe as dd
import pandas as pd
from typing import List


class DaskDataset(PandasDataset):
    def __init__(self, data=dd.from_pandas(pd.DataFrame(), npartitions=1), columns = None):
        self._data = data
        if columns and self._data.empty:
            self._data.columns = columns

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    def __getitem__(self, item):
        return self._data[item].compute()

    def __setitem__(self, key, value):
        if isinstance(value, list):
            self._data[key] = dd.from_pandas(pd.Series(value), npartitions=self._data.npartitions)
        else:
            self._data[key] = value

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        dataframe = dd.from_dict(data, npartitions=1, **kwargs)
        return cls(dataframe)

    @classmethod
    def from_records(cls, data: List[dict], **kwargs):
        data = pd.DataFrame.from_records(data)
        dataframe = dd.from_pandas(data, npartitions=1)
        return cls(dataframe)

    def get(self, column: str, default=None):
        if column in self._data:
            return self._data[column].compute()
        return default

    def apply(self, func, **kwargs):
        return self._data.apply(func, **kwargs).compute()

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
    
    def rename(self, index = None, columns = None, inplace = True):
        self._data.rename(index=index, columns=columns, inplace=inplace)
        return self

    def drop(self, labels=None, axis=0, columns=None, errors='raise'):
        """
        Drop specified labels from rows or columns.
        """
        self._data.drop(labels=labels, axis=axis, columns=columns, errors=errors)
        return self

    def melt(self, id_vars=None, value_vars=None, var_name=None, value_name='value', col_level=None):
        """
        Unpivots a DataFrame from wide format to long format, optionally leaving identifier variables set.
        """
        new_data = self._data.melt(id_vars=id_vars, value_vars=value_vars, value_name=value_name, col_level=col_level)
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
            is_equal = is_equal & self[column].eq(other_dataset[column]).all()
        return is_equal

    def get_error_rows(self, results) -> "pd.Dataframe":
        """
        Returns a pandas dataframe with all errors found in the dataset. Limited to 1000
        """
        results_frame = results.to_frame()
        results_frame.columns = ["results"]
        data_with_results = self.data.merge(results_frame)
        return data_with_results[data_with_results["results"] == True].head(1000)
