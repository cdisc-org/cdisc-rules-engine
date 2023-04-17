import os
from io import IOBase
from typing import List, Optional, Tuple

import pandas

from cdisc_rules_engine.interfaces import CacheServiceInterface, ConfigInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.models.variable_metadata_container import (
    VariableMetadataContainer,
)
from cdisc_rules_engine.services.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from cdisc_rules_engine.services.dataset_metadata_reader import DatasetMetadataReader
from cdisc_rules_engine.utilities.utils import (
    convert_file_size,
    extract_file_name_from_path_string,
)
from .base_data_service import BaseDataService, cached_dataset


class LocalDataService(BaseDataService):
    _instance = None

    @classmethod
    def get_instance(
        cls,
        cache_service: CacheServiceInterface,
        config: ConfigInterface = None,
        **kwargs
    ):
        if cls._instance is None:
            service = cls(
                cache_service=cache_service,
                reader_factory=DataReaderFactory(),
                config=config,
                **kwargs
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

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_dataset(self, dataset_name: str, **params) -> pandas.DataFrame:
        reader = self._reader_factory.get_service()
        df = reader.from_file(dataset_name)
        self._replace_nans_in_numeric_cols_with_none(df)
        return df

    @cached_dataset(DatasetTypes.METADATA.value)
    def get_dataset_metadata(
        self, dataset_name: str, size_unit: str = None, **params
    ) -> pandas.DataFrame:
        """
        Gets metadata of a dataset and returns it as a DataFrame.
        """
        file_metadata, contents_metadata = self.__get_dataset_metadata(
            dataset_name, size_unit=size_unit, **params
        )
        metadata_to_return: dict = {
            "dataset_size": [file_metadata["size"]],
            "dataset_location": [file_metadata["name"]],
            "dataset_name": [contents_metadata["dataset_name"]],
            "dataset_label": [contents_metadata["dataset_label"]],
        }
        return pandas.DataFrame.from_dict(metadata_to_return)

    @cached_dataset(DatasetTypes.RAW_METADATA.value)
    def get_raw_dataset_metadata(self, dataset_name: str, **kwargs) -> DatasetMetadata:
        """
        Returns dataset metadata as DatasetMetadata instance.
        """
        file_metadata, contents_metadata = self.__get_dataset_metadata(
            dataset_name, **kwargs
        )
        return DatasetMetadata(
            name=contents_metadata["dataset_name"],
            domain_name=contents_metadata["domain_name"],
            label=contents_metadata["dataset_label"],
            modification_date=contents_metadata["dataset_modification_date"],
            filename=file_metadata["name"],
            full_path=file_metadata["path"],
            size=file_metadata["size"],
            records="",
        )

    @cached_dataset(DatasetTypes.VARIABLES_METADATA.value)
    def get_variables_metadata(self, dataset_name: str, **params) -> pandas.DataFrame:
        """
        Gets dataset from blob storage and returns metadata of a certain variable.
        """
        metadata: dict = self.read_metadata(dataset_name)
        contents_metadata: dict = metadata["contents_metadata"]
        metadata_to_return: VariableMetadataContainer = VariableMetadataContainer(
            contents_metadata
        )
        return pandas.DataFrame.from_dict(metadata_to_return.to_representation())

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        """
        Reads local define xml file as bytes
        """
        with open(dataset_name, "rb") as f:
            return f.read()

    def get_dataset_by_type(
        self, dataset_name: str, dataset_type: str, **params
    ) -> pandas.DataFrame:
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

    def read_metadata(self, file_path: str) -> dict:
        file_size = os.path.getsize(file_path)
        file_name = extract_file_name_from_path_string(file_path)
        file_metadata = {
            "path": file_path,
            "name": file_name,
            "size": file_size,
        }
        with open(file_path, "rb") as f:
            contents_metadata = DatasetMetadataReader(f.read(), file_name).read()

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
        metadata: dict = self.read_metadata(dataset_name)
        file_metadata: dict = metadata["file_metadata"]
        size_unit: Optional[str] = kwargs.get("size_unit")
        if size_unit:  # convert file size from bytes to desired unit if needed
            file_metadata["size"] = convert_file_size(file_metadata["size"], size_unit)
        return file_metadata, metadata["contents_metadata"]
