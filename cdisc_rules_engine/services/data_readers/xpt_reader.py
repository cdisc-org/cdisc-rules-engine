from io import BytesIO

import pandas as pd
from cdisc_rules_engine.models.dataset import PandasDataset
import tempfile

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)
from cdisc_rules_engine.exceptions import UnsupportedXptFormatError


class XPTReader(DataReaderInterface):

    def _ensure_supported_transport_version(self, data):
        try:
            pd.read_sas(BytesIO(data), format="xport", encoding=self.encoding)
        except Exception as exc:
            raise UnsupportedXptFormatError(
                f"Unsupported XPT (SAS Transport) format. Only Transport v5 is supported. Original error: {exc}"
            ) from exc

    def read(self, data):
        self._ensure_supported_transport_version(data)
        df = pd.read_sas(BytesIO(data), format="xport", encoding=self.encoding)
        df = self._format_floats(df)
        return df

    def _read_pandas(self, file_path):
        with open(file_path, "rb") as f:
            raw = f.read(4096)
        self._ensure_supported_transport_version(raw)
        data = pd.read_sas(file_path, format="xport", encoding=self.encoding)
        return PandasDataset(self._format_floats(data))

    def to_parquet(self, file_path: str) -> str:
        with open(file_path, "rb") as f:
            raw = f.read(4096)
        self._ensure_supported_transport_version(raw)

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        dataset = pd.read_sas(file_path, chunksize=20000, encoding=self.encoding)
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
