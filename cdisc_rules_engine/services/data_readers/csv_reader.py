import logging
import tempfile
from datetime import datetime
from pathlib import Path

from cdisc_rules_engine.constants import DEFAULT_ENCODING
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


class DatasetCSVMetadataReader:
    def __init__(
        self, file_path: str, file_name: str, encoding: str = DEFAULT_ENCODING
    ):
        self.file_path = file_path
        self.file_name = file_name
        self.encoding = encoding

    def read(self) -> dict:
        dataset_name = Path(self.file_name).stem.lower()

        variables_file_path = Path(self.file_path).parent / "variables.csv"

        if not variables_file_path.exists():
            logger = logging.getLogger("validator")
            logger.warning("No variables file found")
            return {}

        meta_df = pd.read_csv(variables_file_path, encoding=self.encoding)

        meta_df["dataset"] = meta_df["dataset"].apply(
            lambda x: Path(str(x)).stem.lower()
        )

        dataset_meta_df = meta_df[meta_df["dataset"] == dataset_name]

        if dataset_meta_df.empty:
            raise ValueError(f"No metadata found for dataset '{dataset_name}'")

        variable_names = dataset_meta_df["variable"].tolist()
        variable_labels = dataset_meta_df["label"].tolist()

        variable_name_to_label_map = dict(zip(variable_names, variable_labels))
        variable_name_to_data_type_map = dict(
            zip(variable_names, dataset_meta_df["type"])
        )
        variable_name_to_size_map = {
            var: (int(length) if pd.notna(length) else None)
            for var, length in zip(variable_names, dataset_meta_df["length"])
        }

        metadata = {
            "dataset_name": dataset_name.upper(),
            "variable_names": variable_names,
            "variable_labels": variable_labels,
            "variable_formats": [""] * len(variable_names),
            "variable_name_to_label_map": variable_name_to_label_map,
            "variable_name_to_data_type_map": variable_name_to_data_type_map,
            "variable_name_to_size_map": variable_name_to_size_map,
            "number_of_variables": len(variable_names),
            "dataset_modification_date": datetime.fromtimestamp(
                Path(self.file_path).stat().st_mtime
            ).isoformat(),
            "adam_info": {
                "categorization_scheme": {},
                "w_indexes": {},
                "period": {},
                "selection_algorithm": {},
            },
        }
        metadata.update(self.__data_meta())
        metadata.update(self.__dataset_label())
        return metadata

    def __dataset_label(self) -> dict:
        tables_file_path = Path(self.file_path).parent / "tables.csv"
        if not tables_file_path.exists():
            return {}

        tables_df = pd.read_csv(tables_file_path, encoding=self.encoding)

        if "Filename" not in tables_df.columns or "Label" not in tables_df.columns:
            return {}

        tables_df["dataset"] = tables_df["Filename"].apply(
            lambda x: Path(str(x)).stem.lower()
        )

        current_dataset = Path(self.file_name).stem.lower()
        match = tables_df[tables_df["dataset"] == current_dataset]

        if match.empty:
            return {}

        return {"dataset_label": str(match.iloc[0]["Label"])}

    def __data_meta(self):
        first_row_df = pd.read_csv(self.file_path, encoding=self.encoding, nrows=1)

        if not first_row_df.empty:
            first_record = first_row_df.iloc[0].fillna("").astype(str).to_dict()
        else:
            first_record = {}

        with open(self.file_path, encoding=self.encoding) as f:
            dataset_length = sum(1 for _ in f) - 1  # subtract header
        return {
            "dataset_length": dataset_length,
            "first_record": first_record,
        }
