from abc import ABC, abstractmethod
from io import IOBase
from typing import Callable, List, Optional
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from .cache_service_interface import CacheServiceInterface


class DataServiceInterface(ABC):
    """
    Interface that defines a set of methods
    that must be implemented by all services
    that download datasets from a certain storage.
    """

    @classmethod
    @abstractmethod
    def get_instance(
        cls, cache_service: CacheServiceInterface, config, **kwargs
    ) -> "DataServiceInterface":
        """
        Creates an instance of data service
        """

    @abstractmethod
    def get_datasets(self) -> List[dict]:
        """
        Gets a list of datasets.
        """

    @abstractmethod
    def get_dataset(self, dataset_name: str, **params) -> DatasetInterface:
        """
        Gets dataset from blob storage.
        """

    @abstractmethod
    def get_dataset_metadata(self, dataset_name: str, **kwargs) -> DatasetInterface:
        """
        Gets dataset metadata and returns it as DatasetInterface.
        """

    @abstractmethod
    def get_raw_dataset_metadata(self, dataset_name: str, **kwargs) -> DatasetMetadata:
        """
        Gets dataset metadata and returns it as DatasetMetadata instance.
        """

    @abstractmethod
    def get_variables_metadata(self, dataset_name: str, **params) -> DatasetInterface:
        """
        Gets variables metadata of a dataset.
        """

    @abstractmethod
    def get_dataset_by_type(
        self, dataset_name: str, dataset_type: str, **params
    ) -> DatasetInterface:
        """
        Generic function to return dataset based on the type.
        dataset_type param can be: contents, metadata, variables_metadata.
        """

    @abstractmethod
    def concat_split_datasets(self, func_to_call: Callable, dataset_names, **kwargs):
        """
        Accepts a list of split dataset filenames,
        downloads all of them and merges into a single DataFrame.
        """

    @abstractmethod
    def get_define_xml_contents(self, dataset_name: str) -> bytes:
        """
        Returns contents of define.xml file.
        """

    @abstractmethod
    def has_all_files(self, prefix: str, file_names: List[str]) -> bool:
        """
        Checks if all files exist
        """

    @abstractmethod
    def get_file_matching_pattern(self, prefix: str, pattern: str) -> str:
        """
        Returns the path to the file if one matches the pattern given, otherwise
        return None.
        """

    @abstractmethod
    def read_data(self, file_path: str) -> IOBase:
        """
        Reads byte data from the given path and returns BinaryIO instance.
        """

    @abstractmethod
    def get_dataset_class(
        self,
        dataset: DatasetInterface,
        file_path: str,
        datasets: List[dict],
        domain: str,
    ) -> Optional[str]:
        """
        Returns dataset class based on its contents
        """

    @abstractmethod
    def to_parquet(self, file_path: str) -> str:
        """
        Converts a given file_path to parquet. Returns path to new file
        """
