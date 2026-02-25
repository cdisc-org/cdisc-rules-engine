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
            data = pd.read_csv(fp, sep=",", header=0, index_col=0, skiprows=4)
        return data


class DatasetCSVMetadataReader:
    def __init__(
        self, file_path: str, file_name: str, encoding: str = DEFAULT_ENCODING
    ):
        self.file_path = file_path
        self.file_name = file_name
        self.encoding = encoding

    def read(self) -> dict:
        raw_df = pd.read_csv(self.file_path, header=None)

        variable_names = raw_df.iloc[0].tolist()
        variable_labels = raw_df.iloc[1].tolist()
        raw_types = raw_df.iloc[2].tolist()
        raw_sizes = raw_df.iloc[3].tolist()

        # Convert types to Char / Num
        variable_name_to_data_type_map = {}
        for name, t in zip(variable_names, raw_types):
            if str(t).lower() == "num":
                variable_name_to_data_type_map[name] = "Num"
            else:
                variable_name_to_data_type_map[name] = "Char"

        # Convert sizes (null for empty or invalid)
        variable_name_to_size_map = {}
        for name, size in zip(variable_names, raw_sizes):
            try:
                variable_name_to_size_map[name] = int(size)
            except (ValueError, TypeError):
                variable_name_to_size_map[name] = None

        variable_formats = [""] * len(variable_names)

        variable_name_to_label_map = dict(zip(variable_names, variable_labels))

        # Data section
        data_df = pd.read_csv(self.file_path, skiprows=4)

        first_record = (
            data_df.iloc[0].fillna("").astype(str).to_dict()
            if not data_df.empty
            else {}
        )

        metadata = {
            "variable_labels": variable_labels,
            "variable_names": variable_names,
            "variable_formats": variable_formats,
            "variable_name_to_label_map": variable_name_to_label_map,
            "variable_name_to_data_type_map": variable_name_to_data_type_map,
            "variable_name_to_size_map": variable_name_to_size_map,
            "number_of_variables": len(variable_names),
            # somehow pass from tables.csv
            "dataset_label": None,
            "dataset_length": len(data_df),
            "first_record": first_record,
            "dataset_name": None,
            "dataset_modification_date": None,
            "adam_info": {
                "categorization_scheme": {},
                "w_indexes": {},
                "period": {},
                "selection_algorithm": {},
            },
        }

        return metadata
