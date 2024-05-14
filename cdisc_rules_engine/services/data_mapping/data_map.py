import json
import os


class GlobalDataMap:
    module_dir = os.path.dirname(__file__)
    filepath = os.path.join(module_dir, "global_data_map.json")

    @staticmethod
    def initialize_file():
        """Ensures the JSON file exists on disk."""
        if not os.path.exists(GlobalDataMap.filepath):
            with open(GlobalDataMap.filepath, "w") as file:
                json.dump({}, file)

    @classmethod
    def add_mapping(cls, parquet_path, original_path):
        """
        Adds a mapping from a parquet file path to its original file path.
        Writes the new mapping to the JSON file.
        """
        data = cls._read_data()
        data[parquet_path] = original_path
        cls._write_data(data)

    @classmethod
    def get_original_path(cls, parquet_path):
        """
        Retrieves the original file path corresponding to a parquet file path.
        Reads the mapping from the JSON file.
        """
        data = cls._read_data()
        return data.get(parquet_path)

    @classmethod
    def _read_data(cls):
        """Helper method to read data from the JSON file."""
        with open(cls.filepath, "r") as file:
            return json.load(file)

    @classmethod
    def _write_data(cls, data):
        """Helper method to write data to the JSON file."""
        with open(cls.filepath, "w") as file:
            json.dump(data, file)

    @classmethod
    def clear_cache(cls):
        """Clears the contents of the JSON file by writing an empty dictionary."""
        cls._write_data({})


GlobalDataMap.initialize_file()
