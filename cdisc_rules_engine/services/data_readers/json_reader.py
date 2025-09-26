from json import load
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidJSONFormat
from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class JSONReader(DataReaderInterface):
    def from_file(self, file_path):
        try:
            with open(file_path, "rb") as fp:
                json = load(fp)
            return json
        except Exception as e:
            raise InvalidJSONFormat(
                f"\n  Error reading JSON from: {file_path}"
                f"\n  {type(e).__name__}: {e}"
            )

    def read(self, data):
        pass
