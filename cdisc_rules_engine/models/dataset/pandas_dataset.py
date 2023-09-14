from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
import pandas as pd


class PandasDataset(DatasetInterface):
    def __init__(self, data: pd.DataFrame):
        self._data = data

    @property
    def data(self):
        return self._data

    @classmethod
    def from_dict(cls, data: dict):
        dataframe = pd.DataFrame.from_dict(data)
        return cls(dataframe)
