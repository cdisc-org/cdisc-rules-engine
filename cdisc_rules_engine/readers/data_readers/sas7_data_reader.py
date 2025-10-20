from typing import Any, Dict, Iterable, List, Tuple

from pandas.io.sas.sas7bdat import SAS7BDATReader

from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)
from cdisc_rules_engine.readers.data_readers.base_data_reader import BaseDataReader


class Sas7DataReader(BaseDataReader):
    """
    Sas7bdat Reader for datasets
    """

    FILE_EXTENSION = ".sas7bdat"
    CHUNKSIZE = 10000

    def __init__(self, file_path: str):
        super().__init__(file_path)

    def read(self) -> Tuple[DatasetMetadata2, Iterable[List[Dict[str, Any]]]]:
        reader = SAS7BDATReader(self.file_path, encoding="utf-8", chunksize=self.CHUNKSIZE)
        metadata = self._extract_metadata(reader)
        chunk_stream = self._read_chunks(reader, metadata)
        return metadata, chunk_stream

    def _get_extension(self):
        return self.FILE_EXTENSION

    def _extract_variable_metadata(self, reader: SAS7BDATReader) -> List[VariableMetadata]:
        """Extract variable-level metadata from SAS reader."""
        variables = []
        for i, col in enumerate(reader.columns):
            type_field = self._decode(col.ctype)
            # Only two types in SAS7bdat: numeric or string
            type_value = "Num" if self._is_numeric_type(type_field) else "Char"
            var_info = VariableMetadata(
                name=self._decode(col.name).strip(),
                label=self._decode(col.label),
                format=self._decode(col.format),
                type=type_value,
                length=col.length,
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
