import tempfile
from pathlib import Path

import dask.dataframe as dd
from numpy import nan

from cdisc_rules_engine.exceptions.custom_exceptions import InvalidCSVFile
from cdisc_rules_engine.interfaces import DataReaderInterface
from cdisc_rules_engine.constants import DEFAULT_ENCODING
import pandas as pd
from cdisc_rules_engine.models.dataset import PandasDataset, DaskDataset


class CSVReader(DataReaderInterface):
    dtype_mapping = {
        "Char": str,
        "Num": float,
        "Boolean": "boolean",
        "Number": float,
        "String": str,
    }

    def __init__(
        self,
        dataset_implementation=PandasDataset,
        encoding: str = DEFAULT_ENCODING,
        variables_csv_path: str = None,
    ):
        super().__init__(
            dataset_implementation=dataset_implementation, encoding=encoding
        )
        self.variables_csv_path = variables_csv_path

    def read(self, data):
        """
        Function for reading data from a specific file type and returning a
        pandas dataframe of the data.
        """
        raise NotImplementedError

    def _get_declared_dtypes(self, file_path: str) -> dict:
        variables_csv_path = (
            Path(self.variables_csv_path)
            if self.variables_csv_path
            else Path(file_path).parent / "_variables.csv"
        )
        try:
            meta_df = pd.read_csv(variables_csv_path, encoding=self.encoding)
        except (UnicodeDecodeError, UnicodeError) as e:
            raise InvalidCSVFile(
                f"\n  Error reading variables metadata from: {variables_csv_path}"
                f"\n  Failed to decode with {self.encoding} encoding: {e}"
                f"\n  Please specify the correct encoding using the -e flag."
            )
        except Exception as e:
            raise InvalidCSVFile(
                f"\n  Error reading variables metadata from: {variables_csv_path}"
                f"\n  {type(e).__name__}: {e}"
            )
        dataset_name = Path(file_path).stem.lower()
        meta_df["dataset"] = meta_df["dataset"].apply(
            lambda x: Path(str(x)).stem.lower()
        )
        dataset_meta_df = meta_df[meta_df["dataset"] == dataset_name]
        if dataset_meta_df.empty:
            return {}
        return {
            row["variable"]: self.dtype_mapping.get(row["type"], str)
            for _, row in dataset_meta_df.iterrows()
        }

    def from_file(self, file_path):
        try:
            declared_dtypes = self._get_declared_dtypes(file_path)
            with open(file_path, "r", encoding=self.encoding) as fp:
                data = pd.read_csv(
                    fp,
                    sep=",",
                    header=0,
                    index_col=False,
                    dtype=declared_dtypes or None,
                    na_values=[""],
                    keep_default_na=False,
                    true_values=["True", "TRUE", "true", "1"],
                    false_values=["False", "FALSE", "false", "0"],
                )
            data = data.replace({nan: None})
            if self.dataset_implementation == PandasDataset:
                return PandasDataset(data)
            else:
                return DaskDataset(
                    dd.from_pandas(data, npartitions=4), length=len(data.index)
                )
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
