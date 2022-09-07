import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, List, TextIO

import pandas

from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.models.variable_metadata_container import (
    VariableMetadataContainer,
)
from cdisc_rules_engine.interfaces import CacheServiceInterface
from cdisc_rules_engine.services.data_readers.data_reader_factory import (
    DataReaderFactory,
)
from cdisc_rules_engine.services.dataset_metadata_reader import DatasetMetadataReader
from cdisc_rules_engine.utilities.utils import (
    convert_file_size,
    extract_file_name_from_path_string,
)

from .base_data_service import BaseDataService, cached_dataset
from cdisc_rules_engine.config import ConfigService


class LocalDataService(BaseDataService):
    _instance = None

    def __init__(self, **params):
        super(LocalDataService, self).__init__(**params)
        self.cache_service = None
        self.reader_factory = DataReaderFactory()

    @classmethod
    def get_instance(
        cls,
        cache_service: CacheServiceInterface,
        config: ConfigService = None,
        **kwargs
    ):
        if cls._instance is None:
            service = cls()
            service.cache_service = cache_service
            cls._instance = service
        return cls._instance

    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        files = [
            f for f in os.listdir(prefix) if os.path.isfile(os.path.join(prefix, f))
        ]
        return all(item in files for item in file_names)

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_dataset(self, dataset_name: str, **params) -> pandas.DataFrame:
        reader = self.reader_factory.get_service()
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
        metadata: dict = self.read_metadata(dataset_name)
        file_metadata: dict = metadata["file_metadata"]
        file_size = file_metadata["size"]
        if size_unit:  # convert file size from bytes to desired unit if needed
            file_size = convert_file_size(file_size, size_unit)
        contents_metadata: dict = metadata["contents_metadata"]
        metadata_to_return: dict = {
            "dataset_size": [file_size],
            "dataset_location": [file_metadata["name"]],
            "dataset_name": [contents_metadata["dataset_name"]],
            "dataset_label": [contents_metadata["dataset_label"]],
        }
        return pandas.DataFrame.from_dict(metadata_to_return)

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
        with os.open(dataset_name, "rb") as f:
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

    def join_split_datasets(
        self, func_to_call: Callable, dataset_names: List[str], **kwargs
    ) -> pandas.DataFrame:
        """
        Accepts a list of split dataset filenames,
        downloads all of them and merges into a single DataFrame.
        """
        # popping drop_duplicates param at the beginning to avoid
        # passing it to func_to_call
        drop_duplicates: bool = kwargs.pop("drop_duplicates", False)
        datasets: Iterator[pandas.DataFrame] = self._async_get_datasets(
            func_to_call, dataset_names, **kwargs
        )
        joined_dataset: pandas.DataFrame = pandas.concat(
            [dataset for dataset in datasets],
            ignore_index=True,
        )
        if drop_duplicates:
            joined_dataset.drop_duplicates()
        return joined_dataset

    def _async_get_datasets(
        self, function_to_call: Callable, dataset_names: List[str], **kwargs
    ) -> Iterator[pandas.DataFrame]:
        """
        The method uses multithreading to download each
        dataset in dataset_names param in parallel.

        function_to_call param is a function that downloads
        one dataset. So, this function is asynchronously called
        for each item of dataset_names param.
        """
        with ThreadPoolExecutor() as executor:
            return executor.map(
                lambda name: function_to_call(dataset_name=name, **kwargs),
                dataset_names,
            )

    def read_metadata(self, file_path: str) -> dict:
        file_size = os.path.getsize(file_path)
        file_metadata = {
            "path": file_path,
            "name": extract_file_name_from_path_string(file_path),
            "size": file_size,
        }
        with open(file_path, "rb") as f:
            contents_metadata = DatasetMetadataReader(f.read()).read()

        return {
            "file_metadata": file_metadata,
            "contents_metadata": contents_metadata,
        }

    def read_data(self, file_path: str, read_mode: str = "r") -> TextIO:
        return open(file_path, read_mode)
