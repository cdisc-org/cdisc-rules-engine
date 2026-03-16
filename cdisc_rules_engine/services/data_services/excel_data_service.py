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
        super(ExcelDataService, self).__init__(
            cache_service, reader_factory, config, **kwargs
        )
        self.dataset_path: str = kwargs.get("dataset_path", "")

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

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_dataset(self, dataset_name: str, **params) -> DatasetInterface:
        dtype_mapping = {
            "Char": str,
            "Num": float,
            "Boolean": "boolean",
            "Number": float,
            "String": str,
        }
        header = pd.read_excel(
            self.dataset_path,
            sheet_name=dataset_name,
            header=None,
            nrows=3,
            na_values=[""],
            keep_default_na=False,
        )
        dtypes = dict(zip(header.iloc[0].tolist(), header.iloc[2].tolist()))
        dtypes = {key: dtype_mapping.get(value, str) for key, value in dtypes.items()}
        dataframe = pd.read_excel(
            self.dataset_path,
            sheet_name=dataset_name,
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
                f"Sheet '{dataset_name}' has column headers with leading/trailing whitespace: "
                f"{[repr(c) for c in offending]}."
            )
        dataset = PandasDataset(dataframe)
        return dataset

    def _get_dataset_name(
        self, metadata: pd.DataFrame, first_record: dict, dataset_filename: str
    ) -> str:
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

    @cached_dataset(DatasetTypes.RAW_METADATA.value)
    def get_raw_dataset_metadata(
        self,
        dataset_name: str,
        **kwargs,
    ) -> SDTMDatasetMetadata:
        """
        Returns dataset metadata as DatasetMetadata instance.
        """
        datasets_worksheet = self._get_datasets_worksheet()
        metadata = datasets_worksheet[
            datasets_worksheet[ExcelDataSheets.DATASET_FILENAME_COLUMN.value]
            == dataset_name
        ]
        dataset = self.get_dataset(dataset_name=dataset_name)
        first_record = dataset.data.iloc[0].to_dict() if not dataset.empty else {}
        return SDTMDatasetMetadata(
            name=self._get_dataset_name(metadata, first_record, dataset_name),
            first_record=first_record,
            label=(
                metadata[ExcelDataSheets.DATASET_LABEL_COLUMN.value].iloc[0]
                if not metadata.empty
                else ""
            ),
            modification_date=datetime.fromtimestamp(
                os.path.getmtime(self.dataset_path)
            ).isoformat(),
            filename=dataset_name,
            full_path=dataset_name,
            file_size=0,
            record_count=len(dataset),
        )

    @cached_dataset(DatasetTypes.VARIABLES_METADATA.value)
    def get_variables_metadata(self, dataset_name: str, **params) -> DatasetInterface:
        """
        Gets dataset from blob storage and returns metadata of a certain variable.
        """
        dataframe = pd.read_excel(
            self.dataset_path,
            sheet_name=dataset_name,
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

    def get_datasets(self) -> List[dict]:
        try:
            with pd.ExcelFile(self.dataset_path) as xl:
                sheet_names = xl.sheet_names
                if ExcelDataSheets.DATASETS_SHEET_NAME.value not in sheet_names:
                    available = ", ".join(repr(s) for s in sheet_names) or "(none)"
                    raise ExcelTestDataError(
                        f"The workbook does not contain a '{ExcelDataSheets.DATASETS_SHEET_NAME.value}' sheet. "
                        f"Submitted sheet names: {available}."
                    )
                worksheet = xl.parse(
                    ExcelDataSheets.DATASETS_SHEET_NAME.value,
                    na_values=[""],
                    keep_default_na=False,
                )
        except ExcelTestDataError:
            raise
        except Exception as e:
            raise ExcelTestDataError(
                f"Cannot read the Excel file. Ensure it is a valid .xlsx workbook. "
                f"Details: {e}"
            ) from e

        missing_cols = sorted(
            set(ExcelDataSheets.DATASETS_SHEET_REQUIRED_COLUMNS.value)
            - set(worksheet.columns)
        )
        if missing_cols:
            raise ExcelTestDataError(
                f"The '{ExcelDataSheets.DATASETS_SHEET_NAME.value}' sheet is missing a "
                f"required {ExcelDataSheets.DATASETS_SHEET_REQUIRED_COLUMNS.value} column(s): "
                f"{missing_cols}. Column headers are case-sensitive. "
            )

        datasets = [
            self.get_raw_dataset_metadata(dataset_name=fn)
            for fn in worksheet[ExcelDataSheets.DATASET_FILENAME_COLUMN.value]
        ]
        return datasets

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
