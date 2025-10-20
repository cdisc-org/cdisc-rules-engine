from cdisc_rules_engine.readers.data_readers.base_data_reader import BaseDataReader
from cdisc_rules_engine.readers.data_readers.sas7_data_reader import Sas7DataReader
from cdisc_rules_engine.readers.data_readers.xpt_data_reader import XptDataReader


class DataReaderFactory:
    _lookup = {XptDataReader.FILE_EXTENSION: XptDataReader, Sas7DataReader.FILE_EXTENSION: Sas7DataReader}

    @staticmethod
    def get_data_reader(file_path: str) -> BaseDataReader:
        extension = file_path.split(".")[-1]
        constructor = DataReaderFactory._lookup.get("." + extension, None)
        if not constructor:
            raise ValueError("No data reader found for dataset file extension: ." + extension)
        return constructor(file_path)
