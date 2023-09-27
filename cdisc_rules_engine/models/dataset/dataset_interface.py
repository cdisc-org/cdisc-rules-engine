from abc import ABC, abstractmethod
from typing import List


class DatasetInterface(ABC):
    @property
    @abstractmethod
    def data(self):
        """
        Stores the underlying data for the dataset
        """

    @property
    @abstractmethod
    def columns(self):
        """
        Stores the columns of the underlying dataset
        """

    @classmethod
    @abstractmethod
    def from_dict(self, data: dict, **kwargs):
        """
        Create the underlying dataset from provided dictionary data
        """

    @classmethod
    @abstractmethod
    def from_records(self, data: List[dict], **kwargs):
        """
        Create the underlying dataset from provided list of records
        """

    @abstractmethod
    def __getitem__(self, item: str):
        """
        Access dataset column by name
        """

    @abstractmethod
    def __setitem__(self, key: str, data):
        """
        Set value of a dataset column
        """

    @abstractmethod
    def __len__(self):
        """
        Get length of dataset
        """

    @abstractmethod
    def __contains__(self, item: str) -> bool:
        """
        Return true if item is in dataset
        """

    @abstractmethod
    def get(self, column: str, default=None):
        """
        Return column if column is in dataset, else return default
        """

    @abstractmethod
    def groupby(self, by: List[str], **kwargs):
        """
        Group dataframe by list of columns.
        """

    @abstractmethod
    def concat(self, other: "DatasetInterface", **kwargs):
        """
        Concat two datasets
        """

    @abstractmethod
    def merge(self, other: "DatasetInterface", **kwargs):
        """
        merge two datasets
        """

    @abstractmethod
    def apply(self, func, **kwargs):
        """
        Apply a function to a dataset
        """

    @abstractmethod
    def iterrows(self):
        """
        Return iterator over all dataset rows
        """

    @abstractmethod
    def is_series(self, data) -> bool:
        """
        Return true if the data is a series compatible with the underlying dataset
        """

    @abstractmethod
    def convert_to_series(self, data):
        """
        Converts list like data to a series corresponding with the underlying dataset
        """

    @abstractmethod
    def get_series_from_value(self, value):
        """
        Create a series of a single value
        """

    @abstractmethod
    def len(self) -> int:
        """
        Return the length of the dataset
        """
