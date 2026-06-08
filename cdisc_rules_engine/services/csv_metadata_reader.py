import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from cdisc_rules_engine.constants import DEFAULT_ENCODING


class DatasetCSVMetadataReader:
    def __init__(
        self,
        file_path: str,
        file_name: str,
        encoding: str = DEFAULT_ENCODING,
        variables_csv_path: str = None,
        datasets_csv_path: str = None,
        **kwargs,
    ):
        self.file_path = file_path
        self.file_name = file_name
        self.dataset = Path(file_name).stem
        self.encoding = encoding
        self.variables_csv_path = (
            Path(variables_csv_path)
            if variables_csv_path
            else Path(self.file_path).parent / "_variables.csv"
        )
        self.datasets_csv_path = (
            Path(datasets_csv_path)
            if datasets_csv_path
            else Path(self.file_path).parent / "_datasets.csv"
        )

    def read(self) -> dict:
        metadata = {}
        metadata.update(self.__dataset_metadata())
        metadata.update(
            {
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
        )
        metadata.update(self.__variable_metadata())
        metadata.update(self.__data_metadata())
        return metadata

    def __dataset_metadata(self) -> dict:
        logger = logging.getLogger("validator")

        if not self.datasets_csv_path.exists():
            logger.info("No datasets file found for %s", self.dataset)
            return {}

        try:
            datasets_df = pd.read_csv(self.datasets_csv_path, encoding=self.encoding)
        except (UnicodeDecodeError, UnicodeError) as e:
            logger.error(
                f"\n  Error reading CSV from: {self.file_path}"
                f"\n  Failed to decode with {self.encoding} encoding: {e}"
                f"\n  Please specify the correct encoding using the -e flag."
            )
            return {}
        except Exception as e:
            logger.error("Error reading CSV file %s. %s", self.file_path, e)
            return {}

        if "Filename" not in datasets_df.columns:
            return {}

        match = datasets_df[datasets_df["Filename"] == self.dataset]

        if match.empty or len(match) > 1:
            return {}

        single_match = match.iloc[0]

        return {
            "dataset_name": (
                single_match["Dataset Name"]
                if "Dataset Name" in datasets_df.columns
                else str(single_match["Filename"]).upper()
            ),
            "dataset_label": str(single_match["Label"]),
        }

    def __variable_metadata(
        self,
    ) -> dict:
        logger = logging.getLogger("validator")
        if not self.variables_csv_path.exists():
            logger.info("No variables file found for %s", self.dataset)
            return {}
        try:
            meta_df = pd.read_csv(self.variables_csv_path, encoding=self.encoding)
        except (UnicodeDecodeError, UnicodeError) as e:
            logger.error(
                f"Could not decode CSV file {self.variables_csv_path} with {self.encoding} encoding: {e}. "
                f"Please specify the correct encoding using the -e flag."
            )
            return {}
        except Exception as e:
            logger.error("Error reading CSV file %s. %s", self.file_path, e)
            return {}

        dataset_meta_df = meta_df[meta_df["dataset"] == self.dataset]

        if dataset_meta_df.empty:
            logger.info("No dataset metadata found for %s", self.dataset)
            return {}

        variable_names = dataset_meta_df["variable"].tolist()
        variable_labels = dataset_meta_df["label"].tolist()

        variable_name_to_label_map = dict(zip(variable_names, variable_labels))
        variable_name_to_data_type_map = dict(
            zip(variable_names, dataset_meta_df["type"])
        )
        variable_name_to_size_map = {
            var: (
                int(length)
                if pd.notna(length) and (isinstance(length, int) or length.isdigit())
                else None
            )
            for var, length in zip(variable_names, dataset_meta_df["length"])
        }
        return {
            "variable_names": variable_names,
            "variable_labels": variable_labels,
            "variable_formats": [""] * len(variable_names),
            "variable_name_to_label_map": variable_name_to_label_map,
            "variable_name_to_data_type_map": variable_name_to_data_type_map,
            "variable_name_to_size_map": variable_name_to_size_map,
            "number_of_variables": len(variable_names),
        }

    def __data_metadata(self):
        logger = logging.getLogger("validator")
        result = {
            "dataset_length": 0,
            "first_record": {},
        }
        try:
            first_row_df = pd.read_csv(self.file_path, encoding=self.encoding, nrows=1)
        except (UnicodeDecodeError, UnicodeError) as e:
            logger.error(
                f"\n  Error reading CSV from: {self.file_path}"
                f"\n  Failed to decode with {self.encoding} encoding: {e}"
                f"\n  Please specify the correct encoding using the -e flag."
            )
            return result
        except Exception as e:
            logger.error("Error reading CSV file %s. %s", self.file_path, e)
            return result

        if not first_row_df.empty:
            result["first_record"] = (
                first_row_df.iloc[0].fillna("").astype(str).to_dict()
            )

        try:
            with open(self.file_path, encoding=self.encoding) as f:
                result["dataset_length"] = max(
                    sum(1 for _ in f) - 1, 0
                )  # subtract header
        except (UnicodeDecodeError, UnicodeError) as e:
            logger.error(
                f"\n  Error reading CSV from: {self.file_path}"
                f"\n  Failed to decode with {self.encoding} encoding: {e}"
                f"\n  Please specify the correct encoding using the -e flag."
            )
        except Exception as e:
            logger.error("Error reading CSV file %s. %s", self.file_path, e)

        return result
