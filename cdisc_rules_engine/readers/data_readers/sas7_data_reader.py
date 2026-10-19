from typing import Iterable, List, Optional, Tuple

import pandas as pd
import pyreadstat
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
    CHUNKSIZE = 200000

    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._pyreadstat_meta = None

    def read(self) -> Tuple[DatasetMetadata2, Iterable[pd.DataFrame]]:
        self._pyreadstat_meta = self._read_pyreadstat_metadata()
        reader = SAS7BDATReader(self.file_path, encoding=None, chunksize=self.CHUNKSIZE, convert_dates=False)
        metadata = self._extract_metadata(reader)
        chunk_stream = self._read_chunks(reader, metadata)
        return metadata, chunk_stream

    def _get_extension(self):
        return self.FILE_EXTENSION

    def _extract_variable_metadata(self, reader: SAS7BDATReader) -> List[VariableMetadata]:
        """Extract variable-level metadata from SAS reader."""
        labels = self._pyreadstat_meta.column_names_to_labels if self._pyreadstat_meta else {}
        variables = []
        for i, col in enumerate(reader.columns):
            type_field = self._decode(col.ctype)
            # Only two types in SAS7bdat: numeric or string
            type_value = "Num" if self._is_numeric_type(type_field) else "Char"
            name = self._decode(col.name).strip()
            label = labels.get(name)
            var_info = VariableMetadata(
                name=name,
                label=self._decode(col.label) if label is None else label,
                format=self._decode(col.format),
                type=type_value,
                length=col.length,
                order=i + 1,
            )
            variables.append(var_info)
        return variables

    def _read_pyreadstat_metadata(self):
        try:
            _, meta = pyreadstat.read_sas7bdat(self.file_path, metadataonly=True)
        except Exception:
            return None
        return meta

    def _extract_label(self, reader: SAS7BDATReader) -> str:
        if self._pyreadstat_meta is None:
            return ""
        return (self._pyreadstat_meta.file_label or "").strip()

    def _get_total_rows(self, reader=None) -> Optional[int]:
        if self._pyreadstat_meta is None:
            return None
        return self._pyreadstat_meta.number_rows
