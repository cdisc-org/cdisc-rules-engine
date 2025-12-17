from io import BytesIO

import pandas as pd
from cdisc_rules_engine.models.dataset import PandasDataset
import tempfile

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class XPTReader(DataReaderInterface):
    def __init__(self, dataset_implementation, encoding: str = None):
        self.dataset_implementation = dataset_implementation
        self.encoding = encoding

    @property
    def _encoding(self):
        return self.encoding or "utf-8"

    def read(self, data):
        df = pd.read_sas(BytesIO(data), format="xport", encoding=self._encoding)
        df = self._format_floats(df)
        return df

    def _read_pandas(self, file_path):
        data = pd.read_sas(file_path, format="xport", encoding=self._encoding)
        return PandasDataset(self._format_floats(data))

    def _read_xpt_with_encoding(self, file_path: str, chunksize: int = None):
        return pd.read_sas(file_path, chunksize=chunksize, encoding=self._encoding)

    def to_parquet(self, file_path: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        dataset = self._read_xpt_with_encoding(file_path, chunksize=20000)
        created = False
        num_rows = 0
        for chunk in dataset:
            chunk = self._format_floats(chunk)
            num_rows += len(chunk)
            if not created:
                chunk.to_parquet(temp_file.name, engine="fastparquet")
                created = True
            else:
                chunk.to_parquet(temp_file.name, engine="fastparquet", append=True)
        return num_rows, temp_file.name

    def from_file(self, file_path):
        return self._read_pandas(file_path)

    def _format_floats(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        return dataframe.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)
