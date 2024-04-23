from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
import pandas as pd
from typing import Union, List


class PandasDataset(DatasetInterface):
    def __init__(self, data: pd.DataFrame = pd.DataFrame(), columns=None):
        self._data = data
        self.length = len(data)
        if columns and self._data.empty:
            self._data = pd.DataFrame(columns=columns)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    @property
    def columns(self):
        return self._data.columns

    @columns.setter
    def columns(self, columns):
        self._data.columns = columns

    @property
    def index(self):
        return self._data.index

    @property
    def groups(self):
        return self._data.groups

    @property
    def empty(self):
        return self._data.empty

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        dataframe = pd.DataFrame.from_dict(data, **kwargs)
        return cls(dataframe)

    @classmethod
    def from_records(cls, data: List[dict], **kwargs):
        dataframe = pd.DataFrame.from_records(data, **kwargs)
        return cls(dataframe)

    def __getitem__(
        self, item: Union[str, List[str]]
    ) -> Union[pd.Series, pd.DataFrame]:
        return self._data[item]

    def __setitem__(self, key, value: pd.Series):
        self._data[key] = value

    def __len__(self):
        return len(self._data)

    def __contains__(self, item: str) -> bool:
        return item in self._data

    def get(self, column: str, default=None):
        if column in self._data:
            return self._data[column]
        return default

    def groupby(self, by: List[str], **kwargs):
        return self.__class__(self._data.groupby(by, **kwargs))

    def concat(self, other: Union[DatasetInterface, List[DatasetInterface]], **kwargs):
        if isinstance(other, list):
            new_data = self._data.copy()
            for dataset in other:
                new_data = pd.concat([new_data, dataset.data], **kwargs)
        else:
            new_data = pd.concat([self._data, other.data], **kwargs)
        return self.__class__(new_data)

    def merge(self, other: DatasetInterface, **kwargs):
        new_data = self._data.merge(other, **kwargs)
        return self.__class__(new_data)

    def apply(self, func, **kwargs):
        return self._data.apply(func, **self._remove_invalid_kwargs(["meta"], kwargs))

    def iterrows(self):
        return self._data.iterrows()

    def is_series(self, data) -> bool:
        return isinstance(data, pd.Series)

    def rename(self, index=None, columns=None, inplace=True):
        self._data.rename(index=index, columns=columns, inplace=inplace)
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

    def set_index(self, keys, **kwargs):
        new_data = self._data.set_index(keys, **kwargs)
        return self.__class__(new_data)

    def filter(self, **kwargs):
        new_data = self._data.filter(**kwargs)
        return self.__class__(new_data)

    def convert_to_series(self, result):
        if self.is_series(result):
            return result
        return pd.Series(result)

    def get_series_from_value(self, result):
        if hasattr(result, "__iter__"):
            return pd.Series([result] * len(self._data), index=self._data.index)
        return pd.Series(result, index=self._data.index)

    def _remove_invalid_kwargs(self, invalid_args, kwargs) -> dict:
        for arg in invalid_args:
            if arg in kwargs:
                del kwargs[arg]

        return kwargs

    def len(self) -> int:
        return self._data.shape[0]

    @property
    def size(self) -> int:
        return self._data.memory_usage().sum()

    def copy(self):
        new_data = self._data.copy()
        return self.__class__(new_data)

    def equals(self, other_dataset: DatasetInterface):
        return self._data.equals(other_dataset.data)

    def get_error_rows(self, results) -> "pd.Dataframe":
        data_with_results = self._data.copy()
        data_with_results["results"] = results
        return data_with_results[data_with_results["results"].isin([True])]

    def where(self, cond, other, **kwargs):
        """
        Wrapper for dataframe where function
        """
        new_data = self._data.where(cond, other, **kwargs)
        return self.__class__(new_data)

    @classmethod
    def cartesian_product(cls, left, right):
        """
        Return the cartesian product of two dataframes
        """
        return cls(left.merge(right, how="cross"))
