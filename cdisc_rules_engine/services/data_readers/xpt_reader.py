from io import BytesIO

import pandas as pd
import dask.dataframe as dd

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class XPTReader(DataReaderInterface):
    def read(self, data, size):
        df = pd.read_sas(BytesIO(data), format="xport", encoding="utf-8")
        df = self._format_floats(df)
        return df

    def from_file(self, file_path, size):
        if size > 1000000:
            df = dd.from_pandas(
                pd.read_sas(file_path, format="xport", encoding="utf-8"), npartitions=2
            )
        else:
            df = pd.read_sas(file_path, format="xport", encoding="utf-8")
        df = self._format_floats(df)
        return df

    def _format_floats(self, dataframe):
        return dataframe.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)
