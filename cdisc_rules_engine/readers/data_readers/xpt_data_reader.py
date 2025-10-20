from typing import Any, Dict, Iterable, List, Tuple

from pandas.io.sas.sas_xport import XportReader

from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)
from cdisc_rules_engine.readers.data_readers.base_data_reader import BaseDataReader


class XptDataReader(BaseDataReader):
    """
    XPT Reader for datasets
    """

    FILE_EXTENSION = ".xpt"
    CHUNKSIZE = 10000

    def __init__(self, file_path: str):
        super().__init__(file_path)

    def read(self) -> Tuple[DatasetMetadata2, Iterable[List[Dict[str, Any]]]]:
        reader = XportReader(self.file_path, encoding="utf-8", chunksize=self.CHUNKSIZE)
        metadata = self._extract_metadata(reader)
        chunk_stream = self._read_chunks(reader, metadata)
        return metadata, chunk_stream

    def _get_extension(self):
        return self.FILE_EXTENSION

    def _extract_variable_metadata(self, reader: XportReader) -> List[VariableMetadata]:
        """Extract variable-level metadata from SAS reader."""
        variables = []
        for i, field in enumerate(reader.fields):
            type_field = self._decode(field.get("ntype"))
            # Only two types in XPT: numeric or string
            type_value = "Num" if self._is_numeric_type(type_field) else "Char"
            var_info = VariableMetadata(
                name=self._decode(field.get("name")).strip(),
                label=self._decode(field.get("label", "")),
                format=self._decode(field.get("nform", "")),
                type=type_value,
                length=field.get("field_length", 0),
                order=i + 1,
            )
            variables.append(var_info)
        return variables

    @staticmethod
    def _decode(input) -> str:
        if isinstance(input, bytes):
            return input.decode("utf-8")
        else:
            return input
