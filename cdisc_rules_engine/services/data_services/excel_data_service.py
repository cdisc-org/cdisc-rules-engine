import os
from io import IOBase
import functools
from typing import List, Sequence
from datetime import datetime
import re
import pandas as pd
from numpy import nan

from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.models.variable_metadata_container import (
    VariableMetadataContainer,
)
from cdisc_rules_engine.exceptions.custom_exceptions import ExcelTestDataError
from cdisc_rules_engine.services.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from .base_data_service import BaseDataService, cached_dataset
from cdisc_rules_engine.enums.excel_test_sheets import (
    ExcelDataSheets,
)


class ExcelDataService(BaseDataService):
    _instance = None

    def __init__(
        self,
        cache_service: CacheServiceInterface,
        reader_factory: DataReaderFactory,
        config: ConfigInterface,
        **kwargs,
    ):
        self.dataset_path: str = kwargs.get("dataset_path", "")
        super(ExcelDataService, self).__init__(
            cache_service, reader_factory, config, **kwargs
        )

    @classmethod
    def get_instance(
        cls,
        cache_service: CacheServiceInterface,
        config: ConfigInterface = None,
        **kwargs,
    ):
        if cls._instance is None:
            service = cls(
                cache_service=cache_service,
                reader_factory=DataReaderFactory(
                    dataset_implementation=kwargs.get(
                        "dataset_implementation", PandasDataset
                    ),
                    encoding=kwargs.get("encoding"),
                ),
                config=config,
                **kwargs,
            )
            cls._instance = service
        return cls._instance

    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        return os.path.isfile(self.dataset_path)

    def get_file_matching_pattern(self, prefix: str, pattern: str) -> str:
        """
        Returns the path to the file if one matches the pattern given, otherwise
        return None.
        """
        for f in os.listdir(prefix):
            if os.path.isfile(os.path.join(prefix, f)) and re.match(pattern, f):
                return f
        return None

    def __get_dataset(self, sheet_name: str) -> DatasetInterface:
        dtype_mapping = {
            "Char": str,
            "Num": float,
            "Boolean": "boolean",
            "Number": float,
            "String": str,
        }
        header = pd.read_excel(
            self.dataset_path,
            sheet_name=sheet_name,
            header=None,
            nrows=3,
            na_values=[""],
            keep_default_na=False,
        )
        dtypes = dict(zip(header.iloc[0].tolist(), header.iloc[2].tolist()))
        dtypes = {key: dtype_mapping.get(value, str) for key, value in dtypes.items()}
        dataframe = pd.read_excel(
            self.dataset_path,
            sheet_name=sheet_name,
            dtype=dtypes,
            skiprows=(1, 2, 3),
            na_values=[""],
            keep_default_na=False,
            true_values=["True", "TRUE", "true", True, 1, "1"],
            false_values=["False", "FALSE", "false", False, 0, "0"],
        )
        dataframe = dataframe.replace({nan: None})
        offending = [col for col in dataframe.columns if col != col.strip()]
        if offending:
            raise ExcelTestDataError(
                f"Sheet '{sheet_name}' has column headers with leading/trailing whitespace: "
                f"{[repr(c) for c in offending]}."
            )
        dataset = PandasDataset(dataframe)
        return dataset

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_dataset(self, dataset_name: str, **params) -> DatasetInterface:
        dataset_metadata = self._datasets_metadata.get(dataset_name)
        if dataset_metadata is None:
            return PandasDataset.from_dict({})
        sheet_name = dataset_metadata.filename
        return self.__get_dataset(sheet_name)

    def _get_dataset_name(self, first_record: dict, dataset_filename: str) -> str:
        if self.standard == "usdm":
            return first_record.get("instanceType", dataset_filename.split(".")[0])
        return dataset_filename.split(".")[0].upper()

    @functools.lru_cache(maxsize=None)
    def _get_datasets_worksheet(self) -> pd.DataFrame:
        return pd.read_excel(
            self.dataset_path,
            sheet_name=ExcelDataSheets.DATASETS_SHEET_NAME.value,
            na_values=[""],
            keep_default_na=False,
        )

    def _initialize_datasets_metadata(self, **kwargs) -> dict[str, SDTMDatasetMetadata]:
        """
        Initialize the dataset metadata by reading metadata for all datasets in the Excel file.

        Returns:
            Dictionary mapping dataset name to SDTMDatasetMetadata
        """
        result = {}
        try:
            datasets_worksheet = self._get_datasets_worksheet()
        except ValueError as e:
            # Pandas raises ValueError when sheet is not found
            if "Worksheet named" in str(e):
                try:
                    with pd.ExcelFile(self.dataset_path) as xl:
                        sheet_names = xl.sheet_names
                        available = ", ".join(repr(s) for s in sheet_names) or "(none)"
                except Exception:
                    available = "(unable to read sheet names)"
                raise ExcelTestDataError(
                    f"The workbook does not contain a '{ExcelDataSheets.DATASETS_SHEET_NAME.value}' sheet. "
                    f"Submitted sheet names: {available}."
                ) from e
            raise

        # Check for required columns
        missing_cols = sorted(
            set(ExcelDataSheets.DATASETS_SHEET_REQUIRED_COLUMNS.value)
            - set(datasets_worksheet.columns)
        )
        if missing_cols:
            raise ExcelTestDataError(
                f"The '{ExcelDataSheets.DATASETS_SHEET_NAME.value}' sheet is missing a "
                f"required {ExcelDataSheets.DATASETS_SHEET_REQUIRED_COLUMNS.value} column(s): "
                f"{missing_cols}. Column headers are case-sensitive. "
            )

        for dataset_filename in datasets_worksheet[
            ExcelDataSheets.DATASET_FILENAME_COLUMN.value
        ]:
            dataset = self.__get_dataset(dataset_filename)
            first_record = dataset.data.iloc[0].to_dict() if not dataset.empty else {}
            metadata_row = datasets_worksheet[
                datasets_worksheet[ExcelDataSheets.DATASET_FILENAME_COLUMN.value]
                == dataset_filename
            ]
            dataset_name = self._get_dataset_name(first_record, dataset_filename)
            dataset_metadata = SDTMDatasetMetadata(
                name=dataset_name,
                first_record=first_record,
                label=(
                    metadata_row[ExcelDataSheets.DATASET_LABEL_COLUMN.value].iloc[0]
                    if not metadata_row.empty
                    else ""
                ),
                modification_date=datetime.fromtimestamp(
                    os.path.getmtime(self.dataset_path)
                ).isoformat(),
                filename=dataset_filename,
                full_path=dataset_filename,
                file_size=0,
                record_count=len(dataset),
            )
            result[dataset_name] = dataset_metadata
        return result

    @cached_dataset(DatasetTypes.VARIABLES_METADATA.value)
    def get_variables_metadata(self, dataset_name: str, **params) -> DatasetInterface:
        """
        Gets dataset from blob storage and returns metadata of a certain variable.
        """
        # Get the sheet name from metadata
        dataset_metadata = self._datasets_metadata.get(dataset_name)
        if dataset_metadata is None:
            return PandasDataset.from_dict({})

        dataframe = pd.read_excel(
            self.dataset_path,
            sheet_name=dataset_metadata.filename,
            header=None,
            nrows=4,
            na_values=[""],
            keep_default_na=False,
        )
        metadata_to_return: VariableMetadataContainer = VariableMetadataContainer(
            {
                "variable_names": dataframe.iloc[0].tolist(),
                "variable_labels": dataframe.iloc[1].tolist(),
                "variable_formats": [""] * dataframe.shape[1],
                "variable_name_to_label_map": dict(
                    zip(dataframe.iloc[0].tolist(), dataframe.iloc[1].tolist())
                ),
                "variable_name_to_data_type_map": dict(
                    zip(dataframe.iloc[0].tolist(), dataframe.iloc[2].tolist())
                ),
                "variable_name_to_size_map": dict(
                    zip(
                        dataframe.iloc[0].tolist(),
                        dataframe.iloc[3].tolist(),
                    )
                ),
                "number_of_variables": dataframe.shape[1],
            }
        )
        return self._reader_factory.dataset_implementation.from_dict(
            metadata_to_return.to_representation()
        )

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        """
        Reads local define xml file as bytes
        """
        with open(dataset_name, "rb") as f:
            return f.read()

    def read_data(self, file_path: str) -> IOBase:
        return open(file_path, "rb")

    def to_parquet(self, file_path: str) -> str:
        """
        Stub implementation to satisfy abstract interface requirements.

        This method exists only to fulfill the abstract method requirement from the parent class.
        While implemented to prevent TypeError, it is not intended to be called in this class.
        Other classes implementing this interface make actual use of to_parquet().
        """
        raise NotImplementedError("to_parquet is not supported for this class")

    @staticmethod
    def is_valid_data(dataset_paths: Sequence[str]):
        return (
            dataset_paths
            and len(dataset_paths) == 1
            and dataset_paths[0].lower().endswith(".xlsx")
        )
