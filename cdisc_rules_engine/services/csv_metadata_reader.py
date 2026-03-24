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
        tables_csv_path: str = None,
    ):
        self.file_path = file_path
        self.file_name = file_name
        self.encoding = encoding
        self.variables_csv_path = (
            Path(variables_csv_path)
            if variables_csv_path
            else Path(self.file_path).parent / "variables.csv"
        )
        self.tables_csv_path = (
            Path(tables_csv_path)
            if tables_csv_path
            else Path(self.file_path).parent / "tables.csv"
        )

    def read(self) -> dict:
        dataset_name = Path(self.file_name).stem.lower()

        if not self.variables_csv_path.exists():
            logger = logging.getLogger("validator")
            logger.info("No variables file found for %s", dataset_name)
            variables_meta = {}
        else:
            variables_meta = self.__get_variable_metadata(
                dataset_name, self.variables_csv_path
            )

        metadata = {
            "dataset_name": dataset_name.upper(),
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
        metadata.update(variables_meta)
        metadata.update(self.__data_meta())
        metadata.update(self.__dataset_label())
        return metadata

    def __get_variable_metadata(
        self, dataset_name: str, variables_file_path: Path
    ) -> dict:
        logger = logging.getLogger("validator")
        try:
            meta_df = pd.read_csv(variables_file_path, encoding=self.encoding)
        except (UnicodeDecodeError, UnicodeError) as e:
            logger.error(
                f"Could not decode CSV file {variables_file_path} with {self.encoding} encoding: {e}. "
                f"Please specify the correct encoding using the -e flag."
            )
            return {}
        except Exception as e:
            logger.error("Error reading CSV file %s. %s", self.file_path, e)
            return {}

        meta_df["dataset"] = meta_df["dataset"].apply(
            lambda x: Path(str(x)).stem.lower()
        )

        dataset_meta_df = meta_df[meta_df["dataset"] == dataset_name]

        if dataset_meta_df.empty:
            logger = logging.getLogger("validator")
            logger.info("No dataset metadata found for %s", dataset_name)
            return {}

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
        return {
            "variable_names": variable_names,
            "variable_labels": variable_labels,
            "variable_formats": [""] * len(variable_names),
            "variable_name_to_label_map": variable_name_to_label_map,
            "variable_name_to_data_type_map": variable_name_to_data_type_map,
            "variable_name_to_size_map": variable_name_to_size_map,
            "number_of_variables": len(variable_names),
        }

    def __dataset_label(self) -> dict:
        logger = logging.getLogger("validator")

        if not self.tables_csv_path.exists():
            return {}

        try:
            tables_df = pd.read_csv(self.tables_csv_path, encoding=self.encoding)
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
