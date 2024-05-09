from io import BytesIO

import pandas as pd
from cdisc_rules_engine.models.dataset import PandasDataset
import tempfile

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class XPTReader(DataReaderInterface):
    def read(self, data):
        df = pd.read_sas(BytesIO(data), format="xport", encoding="utf-8")
        df = self._format_floats(df)
        return df

    def _read_pandas(self, file_path):
        data = pd.read_sas(file_path, format="xport", encoding="utf-8")
        return PandasDataset(self._format_floats(data))

    def to_parquet(self, file_path: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        dataset = pd.read_sas(file_path, chunksize=20000, encoding="utf-8")
        created = False
        num_rows = 0
        for chunk in dataset:
            self._format_floats(chunk)
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
