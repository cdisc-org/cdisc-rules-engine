from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import dask.dataframe as dd
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
