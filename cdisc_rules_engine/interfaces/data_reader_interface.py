from abc import ABC


class DataReaderInterface(ABC):
    """
    Interface for reading binary data from different file typs into pandas dataframes
    """

    def __init__(self):
        pass

    def read(self, data):
        """
        Function for reading data from a specific file type and returning a
        pandas dataframe of the data.
        """
        raise NotImplementedError

    def from_file(self, file_path):
        raise NotImplementedError
