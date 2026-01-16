from cdisc_rules_engine.models.dataset import PandasDataset


class DataReaderInterface:
    """
    Interface for reading binary data from different file typs into pandas dataframes
    """

    def __init__(self, dataset_implementation=PandasDataset, encoding: str = "utf-8"):
        """
        :param dataset_implementation DatasetInterface: The dataset type to return.
        :param encoding str: The encoding to use when reading files. Defaults to utf-8.
        """
        self.dataset_implementation = dataset_implementation
        self.encoding = encoding

    def read(self, data):
        """
        Function for reading data from a specific file type and returning a
        pandas dataframe of the data.
        """
        raise NotImplementedError

    def from_file(self, file_path):
        raise NotImplementedError
