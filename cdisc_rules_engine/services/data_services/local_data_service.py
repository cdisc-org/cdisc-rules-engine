import os
from io import IOBase
from typing import Iterable, List, Optional, Tuple

from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.models.variable_metadata_container import (
    VariableMetadataContainer,
)
from cdisc_rules_engine.services.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from cdisc_rules_engine.services.datasetxpt_metadata_reader import (
    DatasetXPTMetadataReader,
)
from cdisc_rules_engine.services.datasetjson_metadata_reader import (
    DatasetJSONMetadataReader,
)
from cdisc_rules_engine.services.datasetndjson_metadata_reader import (
    DatasetNDJSONMetadataReader,
)
from cdisc_rules_engine.utilities.utils import (
    convert_file_size,
    extract_file_name_from_path_string,
)
from .base_data_service import BaseDataService, cached_dataset
from cdisc_rules_engine.enums.dataformat_types import DataFormatTypes
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset import PandasDataset
import re


class LocalDataService(BaseDataService):
    _instance = None

    def __init__(
        self,
        cache_service: CacheServiceInterface,
        reader_factory: DataReaderFactory,
        config: ConfigInterface,
        **kwargs,
    ):
        super(LocalDataService, self).__init__(
            cache_service, reader_factory, config, **kwargs
        )
        self.dataset_paths: Iterable[str] = kwargs.get("dataset_paths", [])

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
        files = [
            f.lower()
            for f in os.listdir(prefix)
            if os.path.isfile(os.path.join(prefix, f))
        ]
        return all(item.lower() in files for item in file_names)

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
        reader = self._reader_factory.get_service(
            extract_file_name_from_path_string(dataset_name).split(".")[1].upper()
        )
        df = reader.from_file(dataset_name)
        return df

    @cached_dataset(DatasetTypes.RAW_METADATA.value)
    def get_raw_dataset_metadata(
        self, dataset_name: str, **kwargs
    ) -> SDTMDatasetMetadata:
        """
        Returns dataset metadata as DatasetMetadata instance.
        """
        file_metadata, contents_metadata = self.__get_dataset_metadata(
            dataset_name, **kwargs
        )
        return SDTMDatasetMetadata(
            name=contents_metadata["dataset_name"],
            first_record=contents_metadata["first_record"],
            label=contents_metadata["dataset_label"],
            modification_date=contents_metadata["dataset_modification_date"],
            filename=file_metadata["name"],
            full_path=file_metadata["path"],
            file_size=file_metadata["file_size"],
            record_count=contents_metadata["dataset_length"],
        )

    @cached_dataset(DatasetTypes.VARIABLES_METADATA.value)
    def get_variables_metadata(
        self, dataset_name: str, datasets: list, **params
    ) -> DatasetInterface:
        """
        Gets dataset from blob storage and returns metadata of a certain variable.
        """
        metadata: dict = self.read_metadata(dataset_name, datasets=datasets)
        contents_metadata: dict = metadata["contents_metadata"]
        metadata_to_return: VariableMetadataContainer = VariableMetadataContainer(
            contents_metadata
        )
        return self.dataset_implementation.from_dict(
            metadata_to_return.to_representation()
        )

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        """
        Reads local define xml file as bytes
        """
        with open(dataset_name, "rb") as f:
            return f.read()

    def get_dataset_by_type(
        self, dataset_name: str, dataset_type: str, **params
    ) -> DatasetInterface:
        """
        Generic function to return dataset based on the type.
        dataset_type param can be: contents, metadata, variables_metadata.
        """
        dataset_type_to_function_map: dict = {
            DatasetTypes.CONTENTS.value: self.get_dataset,
            DatasetTypes.METADATA.value: self.get_dataset_metadata,
            DatasetTypes.VARIABLES_METADATA.value: self.get_variables_metadata,
        }
        return dataset_type_to_function_map[dataset_type](
            dataset_name=dataset_name, **params
        )

    def read_metadata(
        self, file_path: str, datasets: Optional[Iterable[SDTMDatasetMetadata]] = None
    ) -> dict:
        file_size = os.path.getsize(file_path)
        file_name = extract_file_name_from_path_string(file_path)
        file_metadata = {
            "path": file_path,
            "name": file_name,
            "file_size": file_size,
        }
        if file_name.endswith(".parquet") and datasets:
            for obj in datasets:
                if obj.full_path == file_path:
                    file_metadata = {
                        "path": obj.original_path,
                        "name": extract_file_name_from_path_string(obj.original_path),
                        "file_size": os.path.getsize(obj.original_path),
                    }
                    file_name = obj.filename
                    break
            # If we reach this line a parquet dataset was provided without a
            # corresponding xpt or json file. This should not happen
            # TODO: Implement a DatasetParquetMetadataReader so we don't have to
            # perform this check.

        _metadata_reader_map = {
            DataFormatTypes.XPT.value: DatasetXPTMetadataReader,
            DataFormatTypes.JSON.value: DatasetJSONMetadataReader,
            DataFormatTypes.NDJSON.value: DatasetNDJSONMetadataReader,
        }

        file_extension = file_name.split(".")[1].upper()
        if file_extension not in _metadata_reader_map:
            supported_formats = ", ".join(_metadata_reader_map.keys())
            raise ValueError(
                f"Unsupported file format '{file_extension}' in file '{file_name}'.\n"
                f"Supported formats: {supported_formats}\n"
                f"Please provide dataset files in SAS V5 XPT, Dataset-JSON (JSON or NDJSON), or Excel (XLSX) format."
            )

        contents_metadata = _metadata_reader_map[file_extension](
            file_metadata["path"], file_name
        ).read()
        return {
            "file_metadata": file_metadata,
            "contents_metadata": contents_metadata,
        }

    def read_data(self, file_path: str) -> IOBase:
        return open(file_path, "rb")

    def __get_dataset_metadata(self, dataset_name: str, **kwargs) -> Tuple[dict, dict]:
        """
        Internal method that gets dataset metadata
        and converts file size if needed.
        """
        metadata: dict = self.read_metadata(dataset_name, kwargs.get("datasets"))
        file_metadata: dict = metadata["file_metadata"]
        size_unit: Optional[str] = kwargs.get("size_unit")
        if size_unit:  # convert file size from bytes to desired unit if needed
            file_metadata["file_size"] = convert_file_size(
                file_metadata["file_size"], size_unit
            )
        return file_metadata, metadata["contents_metadata"]

    def to_parquet(self, file_path: str) -> str:
        reader = self._reader_factory.get_service(
            extract_file_name_from_path_string(file_path).split(".")[1].upper()
        )
        return reader.to_parquet(file_path)

    def get_datasets(self) -> List[dict]:
        datasets = [
            self.get_raw_dataset_metadata(dataset_name=dataset_path)
            for dataset_path in self.dataset_paths
        ]
        return datasets

    @staticmethod
    def is_valid_data(dataset_paths: List[str]) -> bool:
        for dataset_path in dataset_paths:
            if not os.path.exists(dataset_path):
                return False
        return len(dataset_paths) > 0
