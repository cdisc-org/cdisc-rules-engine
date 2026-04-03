from cdisc_rules_engine.models.dataset import PandasDataset
from cdisc_rules_engine.constants import DEFAULT_ENCODING


class DataReaderInterface:
    """
    Interface for reading binary data from different file typs into pandas dataframes
    """

    def __init__(
        self, dataset_implementation=PandasDataset, encoding: str = DEFAULT_ENCODING
    ):
        """
        :param DatasetInterface dataset_implementation : The dataset type to return.
        :param str encoding : The encoding to use when reading files. Defaults to DEFAULT_ENCODING (e.g. utf-8).
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

    def to_parquet(self, file_path) -> tuple[int, str]:
        """Returns number of rows and path to the parquet file"""
        raise NotImplementedError
