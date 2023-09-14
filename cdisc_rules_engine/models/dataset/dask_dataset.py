from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
import dask.dataframe as dd


class DaskDataset(PandasDataset):
    def __init__(self, data):
        self._data = data

    @property
    def data(self):
        return self._data

    @classmethod
    def from_dict(cls, data: dict):
        dataframe = dd.from_dict(data, npartitions=1)
        return cls(dataframe)
