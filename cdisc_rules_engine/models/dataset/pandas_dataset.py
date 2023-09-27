from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
import pandas as pd
from typing import Union, List


class PandasDataset(DatasetInterface):
    def __init__(self, data: pd.DataFrame = pd.DataFrame()):
        self._data = data

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    @property
    def columns(self):
        return self._data.columns

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
