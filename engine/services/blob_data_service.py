from concurrent.futures import ThreadPoolExecutor
from typing import List, Callable, Iterator

import pandas

from engine.models.dataset_types import DatasetTypes
from engine.services.base_data_service import BaseDataService, cached_dataset
from engine.services.blob_storage_service import BlobStorageService
from engine.utilities.utils import convert_file_size
from engine.services.data_readers.data_reader_factory import DataReaderFactory
from engine.models.variable_metadata_container import VariableMetadataContainer

class BlobDataService(BaseDataService):
    _instance = None

    def __init__(self, **params):
        self.blob_service = None
        self.cache_service = None
        self.reader_factory = DataReaderFactory()

    @classmethod
    def get_instance(cls, **params):
        if cls._instance is None:
            service = cls()
            service.blob_service = BlobStorageService(
                params.get("blob_storage_connection_string"), params.get("container")
            )
            service.cache_service = params["cache_service"]
            cls._instance = service
        return cls._instance

    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        blobs = self.blob_service.get_all_file_names(prefix)
        return all(item in blobs for item in file_names)

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_dataset(self, dataset_name: str, **params) -> pandas.DataFrame:
        data = self.blob_service.read_data(dataset_name)
        reader = self.reader_factory.get_reader()
        df = reader.read(data)
        self._replace_nans_in_numeric_cols_with_none(df)
        return df

    @cached_dataset(DatasetTypes.METADATA.value)
    def get_dataset_metadata(
        self, dataset_name: str, size_unit: str = None, **params
    ) -> pandas.DataFrame:
        """
        Gets metadata of a dataset and returns it as a DataFrame.
        """
        metadata: dict = self.blob_service.read_metadata(dataset_name)
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
        metadata: dict = self.blob_service.read_metadata(dataset_name)
        contents_metadata: dict = metadata["contents_metadata"]
        metadata_to_return: VariableMetadataContainer = VariableMetadataContainer(contents_metadata)
        return pandas.DataFrame.from_dict(metadata_to_return.to_representation())

    @cached_dataset(DatasetTypes.CONTENTS.value)
    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        """
        Downloads define XML from blob storage and
        returns its contents as bytes.
        """
        return self.blob_service.read_data(dataset_name)

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
        # popping drop_duplicates param at the beginning to avoid passing it to func_to_call
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
