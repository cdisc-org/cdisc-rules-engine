from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import dask.dataframe as dd
import numpy as np
import pandas as pd
from typing import List


class DaskDataset(PandasDataset):
    def __init__(self, data=dd.from_pandas(pd.DataFrame(), npartitions=1)):
        self._data = data

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
            self._data[key] = dd.from_pandas(pd.Series(value), npartitions=1)
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
        for arg in invalid_kwargs:
            del kwargs[arg]
        return self._data.groupby(by, **kwargs)

    def is_series(self, data) -> bool:
        return isinstance(data, dd.Series)

    def convert_to_series(self, result):
        if self.is_series(result):
            return result
        elif isinstance(result, np.ndarray):
            return dd.from_array(result)
        elif isinstance(result, pd.Series):
            return dd.from_pandas(result, npartitions=1)
        return pd.Series(result)
