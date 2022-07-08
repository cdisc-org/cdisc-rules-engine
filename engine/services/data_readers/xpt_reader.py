import pandas as pd
from engine.services.data_readers.data_reader_interface import DataReaderInterface
from io import BytesIO

class XPTReader(DataReaderInterface):

    def read(self, data):
        df = pd.read_sas(BytesIO(data), format="xport", encoding='utf-8')
        df = self._format_floats(df)
        return df

    def from_file(self, file_path):
        df = pd.read_sas(file_path, format="xport", encoding='utf-8')
        df = self._format_floats(df)
        return df

    def _format_floats(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        return dataframe.applymap(lambda x: round(x, 15) if isinstance(x, float) else x)