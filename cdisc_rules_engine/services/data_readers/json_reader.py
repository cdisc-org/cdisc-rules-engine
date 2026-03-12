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
            self._detect_whitespace_in_dataset_keys(json_data, file_path)
            return json_data
        except InvalidJSONFormat:
            raise
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

    def _detect_whitespace_in_dataset_keys(self, json_data: dict, file_path: str):
        offending = []
        for dataset in json_data.get("datasets", []):
            dataset_name = dataset.get("filename")
            records = dataset.get("records", {})
            for key in records:
                if key != key.strip():
                    offending.append(f"    dataset '{dataset_name}': {repr(key)}")
        if offending:
            offending_list = "\n".join(offending)
            raise InvalidJSONFormat(
                f"\n  Error reading JSON from: {file_path}"
                f"\n  The following column keys contain leading/trailing whitespace:"
                f"\n{offending_list}"
            )

    def read(self, data):
        pass
