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

    def read(self, data):
        if self.encoding:
            df = pd.read_sas(BytesIO(data), format="xport", encoding=self.encoding)
        else:
            encodings_to_try = ["utf-8", "utf-16", "utf-32", "cp1252", "latin-1"]
            last_error = None

            for encoding in encodings_to_try:
                try:
                    df = pd.read_sas(BytesIO(data), format="xport", encoding=encoding)
                    break
                except UnicodeDecodeError as e:
                    last_error = e
                    continue
            else:
                if last_error:
                    raise last_error
                raise UnicodeDecodeError(
                    "utf-8", b"", 0, 1, "Could not decode XPT data with any encoding"
                )

        df = self._format_floats(df)
        return df

    def _read_pandas(self, file_path):
        if self.encoding:
            data = pd.read_sas(file_path, format="xport", encoding=self.encoding)
        else:
            encodings_to_try = ["utf-8", "utf-16", "utf-32", "cp1252", "latin-1"]
            last_error = None

            for encoding in encodings_to_try:
                try:
                    data = pd.read_sas(file_path, format="xport", encoding=encoding)
                    break
                except UnicodeDecodeError as e:
                    last_error = e
                    continue
            else:
                if last_error:
                    raise last_error
                raise UnicodeDecodeError(
                    "utf-8", b"", 0, 1, "Could not decode XPT file with any encoding"
                )

        return PandasDataset(self._format_floats(data))

    def _read_xpt_with_encoding(self, file_path: str, chunksize: int = None):
        if self.encoding:
            return pd.read_sas(file_path, chunksize=chunksize, encoding=self.encoding)

        encodings_to_try = ["utf-8", "utf-16", "utf-32", "cp1252", "latin-1"]
        last_error = None

        for encoding in encodings_to_try:
            try:
                return pd.read_sas(file_path, chunksize=chunksize, encoding=encoding)
            except UnicodeDecodeError as e:
                last_error = e
                continue
        else:
            if last_error:
                raise last_error
            raise UnicodeDecodeError(
                "utf-8", b"", 0, 1, "Could not decode XPT file with any encoding"
            )

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
