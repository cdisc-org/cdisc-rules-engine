from json import load
from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class USDMJSONReader(DataReaderInterface):
    def from_file(self, file_path):
        with open(file_path) as fp:
            json = load(fp)
        return json

    def read(self, data):
        pass
