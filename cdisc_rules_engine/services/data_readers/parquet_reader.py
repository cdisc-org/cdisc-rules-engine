from io import BytesIO
from typing import Union

import pandas as pd
import dask.dataframe as dd
from cdisc_rules_engine.models.dataset import PandasDataset, DaskDataset

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class ParquetReader(DataReaderInterface):
    def read(self, data):
        df = pd.read_parquet(BytesIO(data), engine="fastparquet", encoding="utf-8")
        df = self._format_floats(df)
        return df

    def _read_pandas(self, file_path):
        data = pd.read_parquet(file_path, engine="fastparquet", encoding="utf-8")
        return PandasDataset(self._format_floats(data))

    def from_file(self, file_path):
        type_to_reader_map = {
            PandasDataset: self._read_pandas,
            DaskDataset: self._read_dask,
        }
        return type_to_reader_map.get(self.dataset_implementation, self._read_pandas)(
            file_path
        )

    def _format_floats(
        self, dataframe: Union[pd.DataFrame, dd.DataFrame]
    ) -> Union[pd.DataFrame, dd.DataFrame]:
        return dataframe.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)

    def _read_dask(self, file_path):
        data = dd.read_parquet(file_path)
        return DaskDataset(data)
