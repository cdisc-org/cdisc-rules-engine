class GlobalDataMap:
    def __init__(self):
        self.parquet_to_original_map = {}

    @classmethod
    def add_mapping(self, parquet_path, original_path):
        """
        Adds a mapping from a parquet file path to its original file path.

        :param parquet_path: The file path of the parquet file.
        :param original_path: The file path of the original file.
        """
        self.parquet_to_original_map[parquet_path] = original_path

    @classmethod
    def get_original_path(self, parquet_path):
        """
        Retrieves the original file path corresponding to a parquet file path.

        :param parquet_path: The file path of the parquet file.
        :return: The original file path or None if no mapping exists.
        """
        return self.parquet_to_original_map.get(parquet_path, None)
