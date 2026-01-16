from json import load
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidJSONFormat
from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class JSONReader(DataReaderInterface):
    def from_file(self, file_path):
        try:
            with open(file_path, "r", encoding=self.encoding) as fp:
                json_data = load(fp)
            return json_data
        except (UnicodeDecodeError, UnicodeError) as e:
            raise InvalidJSONFormat(
                f"\n  Error reading JSON from: {file_path}"
                f"\n  Failed to decode with {self.encoding} encoding: {e}"
                f"\n  Please specify the correct encoding using the -e flag."
            )
        except Exception as e:
            raise InvalidJSONFormat(
                f"\n  Error reading JSON from: {file_path}"
                f"\n  {type(e).__name__}: {e}"
            )

    def read(self, data):
        pass
