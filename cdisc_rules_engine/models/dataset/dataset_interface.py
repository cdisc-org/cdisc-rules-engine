from abc import ABC, abstractmethod


class DatasetInterface(ABC):
    @property
    @abstractmethod
    def data(self):
        """
        Stores the underlying data for the dataset
        """

    @classmethod
    @abstractmethod
    def from_dict(self, data):
        """
        Create the underlying dataset from provided dictionary data
        """
