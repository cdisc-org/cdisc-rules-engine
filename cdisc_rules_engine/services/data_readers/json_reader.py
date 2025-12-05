from json import load
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidJSONFormat
from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)


class JSONReader(DataReaderInterface):
    def from_file(self, file_path, encoding: str = None):
        try:
            if encoding:
                with open(file_path, "r", encoding=encoding) as fp:
                    json = load(fp)
                return json

            encodings_to_try = ["utf-8", "utf-16", "utf-32"]
            last_error = None

            for enc in encodings_to_try:
                try:
                    with open(file_path, "r", encoding=enc) as fp:
                        json = load(fp)
                    return json
                except (UnicodeDecodeError, UnicodeError) as e:
                    last_error = e
                    continue
            else:
                if last_error:
                    raise last_error
                raise InvalidJSONFormat(
                    f"\n  Error reading JSON from: {file_path}"
                    f"\n  Could not decode file with UTF-8, UTF-16, or UTF-32 encoding"
                )
        except Exception as e:
            raise InvalidJSONFormat(
                f"\n  Error reading JSON from: {file_path}"
                f"\n  {type(e).__name__}: {e}"
            )

    def read(self, data):
        pass
