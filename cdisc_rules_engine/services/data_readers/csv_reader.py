import tempfile

from cdisc_rules_engine.interfaces import DataReaderInterface
import pandas as pd


class CSVReader(DataReaderInterface):
    def read(self, data):
        """
        Function for reading data from a specific file type and returning a
        pandas dataframe of the data.
        """
        raise NotImplementedError

    def from_file(self, file_path):
        with open(file_path, "r", encoding=self.encoding) as fp:
            data = pd.read_csv(fp, sep=",", header=0, index_col=False)
        return data

    def to_parquet(self, file_path: str) -> tuple[int, str]:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")

        dataset = pd.read_csv(file_path, chunksize=20000, encoding=self.encoding)

        created = False
        num_rows = 0

        for chunk in dataset:
            num_rows += len(chunk)

            if not created:
                chunk.to_parquet(temp_file.name, engine="fastparquet")
                created = True
            else:
                chunk.to_parquet(temp_file.name, engine="fastparquet", append=True)

        return num_rows, temp_file.name
