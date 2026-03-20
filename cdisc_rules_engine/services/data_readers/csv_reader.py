import tempfile

from cdisc_rules_engine.exceptions.custom_exceptions import InvalidCSVFile
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
        try:
            with open(file_path, "r", encoding=self.encoding) as fp:
                data = pd.read_csv(fp, sep=",", header=0, index_col=False)
            return data
        except (UnicodeDecodeError, UnicodeError) as e:
            raise InvalidCSVFile(
                f"\n  Error reading CSV from: {file_path}"
                f"\n  Failed to decode with {self.encoding} encoding: {e}"
                f"\n  Please specify the correct encoding using the -e flag."
            )
        except Exception as e:
            raise InvalidCSVFile(
                f"\n  Error reading CSV from: {file_path}"
                f"\n  {type(e).__name__}: {e}"
            )

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

        if not created:
            empty_df = pd.read_csv(file_path, nrows=0, encoding=self.encoding)
            empty_df.to_parquet(temp_file.name, engine="fastparquet")

        return num_rows, temp_file.name
