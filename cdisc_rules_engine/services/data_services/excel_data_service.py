import os
from io import IOBase
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
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from .base_data_service import BaseDataService, cached_dataset

DATASETS_SHEET_NAME = "Datasets"
DATASET_FILENAME_COLUMN = "Filename"
DATASET_LABEL_COLUMN = "Label"


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
                    )
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
            "Boolean": bool,
            "Number": float,
            "String": str,
        }
        header = pd.read_excel(
            self.dataset_path, sheet_name=dataset_name, header=None, nrows=3
        )
        dtypes = dict(zip(header.iloc[0].tolist(), header.iloc[2].tolist()))
        dtypes = {key: dtype_mapping.get(value, str) for key, value in dtypes.items()}
        dataframe = pd.read_excel(
            self.dataset_path,
            sheet_name=dataset_name,
            dtype=dtypes,
            skiprows=(1, 2, 3),
        )
        dataframe = dataframe.replace({nan: None})
        dataset = PandasDataset(dataframe)
        return dataset

    @cached_dataset(DatasetTypes.RAW_METADATA.value)
    def get_raw_dataset_metadata(
        self, dataset_name: str, **kwargs
    ) -> SDTMDatasetMetadata:
        """
        Returns dataset metadata as DatasetMetadata instance.
        """
        datasets_worksheet = pd.read_excel(
            self.dataset_path, sheet_name=DATASETS_SHEET_NAME
        )
        metadata = datasets_worksheet[
            datasets_worksheet[DATASET_FILENAME_COLUMN] == dataset_name
        ]
        dataset = self.get_dataset(dataset_name=dataset_name)
        return SDTMDatasetMetadata(
            name=dataset_name.split(".")[0].upper(),
            first_record=(dataset.data.iloc[0].to_dict() if not dataset.empty else {}),
            label=metadata[DATASET_LABEL_COLUMN].iloc[0] if not metadata.empty else "",
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
            self.dataset_path, sheet_name=dataset_name, header=None, nrows=4
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
            worksheet = pd.read_excel(self.dataset_path, sheet_name=DATASETS_SHEET_NAME)
        except TypeError as e:
            logger.error(
                f"Failed to read datasets from the Excel file at {self.dataset_path}. "
                f"Ensure the file is in the correct format. "
                f"Try opening and saving the file in Microsoft Excel. "
                f"Error: {str(e)}"
            )
            raise
        datasets = [
            self.get_raw_dataset_metadata(dataset_name=dataset_filename)
            for dataset_filename in worksheet[DATASET_FILENAME_COLUMN]
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
