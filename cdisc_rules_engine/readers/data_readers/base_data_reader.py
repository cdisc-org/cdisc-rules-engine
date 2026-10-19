from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa

from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)


class BaseDataReader(ABC):
    """
    Abstract Reader for datasets
    """

    CHUNKSIZE = 10000

    SUPPORTED_EXTENSIONS = [".xpt", ".sas7bdat"]

    def __init__(self, file_path: str):
        if not file_path.lower().endswith(self._get_extension()):
            raise ValueError("Tried to read file with incorrect data reader")
        self.file_path = file_path

    @abstractmethod
    def read(self) -> Tuple[DatasetMetadata2, Iterable[Union[pd.DataFrame, pa.Table]]]:
        pass

    @abstractmethod
    def _get_extension(self) -> str:
        pass

    @abstractmethod
    def _extract_variable_metadata(self, reader) -> List[VariableMetadata]:
        pass

    def _get_total_rows(self, reader=None) -> Optional[int]:
        return None

    def _extract_label(self, reader) -> str:
        return ""

    def _extract_metadata(self, reader) -> Dict[str, Any]:
        """Read only the metadata."""
        path = Path(self.file_path)
        stat = path.stat()
        return DatasetMetadata2(
            filename=path.name.lower(),
            name=path.name.lower().replace(self._get_extension(), ""),
            variables=self._extract_variable_metadata(reader),
            label=self._extract_label(reader),
            file_size=stat.st_size,
            full_path=str(path.resolve()),
            modification_date=datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0).isoformat(),
        )

    def _read_chunks(self, reader, metadata: DatasetMetadata2) -> Iterable[pd.DataFrame]:
        for chunk in reader:
            chunk = self._clean_chunk(chunk, metadata)
            chunk.columns = [column.lower() for column in chunk.columns]
            yield chunk

    def _clean_chunk(self, frame: pd.DataFrame, metadata: DatasetMetadata2) -> pd.DataFrame:
        """Normalise a chunk per col."""
        for column in metadata.variables:
            values = frame[column.name]
            if column.type == "Char":
                frame[column.name] = self._decode_column(values).fillna("")
            else:
                values = pd.to_numeric(values, errors="coerce")
                frame[column.name] = values.mask(values.abs() < 1e-20, 0.0)
        return frame

    def _decode_column(self, values: pd.Series) -> pd.Series:
        if values.dtype != object or values.empty:
            return values

        is_bytes = values.map(lambda value: isinstance(value, bytes))
        if not is_bytes.any():
            return values
        if not is_bytes[values.notna()].all():
            # str.decode would turn the values that are already text into NaN
            return values.map(self._decode)

        try:
            return values.str.decode("utf-8")
        except UnicodeDecodeError:
            return values.map(self._decode)

    @staticmethod
    def _decode(value: Any) -> str:
        if not isinstance(value, bytes):
            return value

        for encoding in ("utf-8", "windows-1252"):
            try:
                return value.decode(encoding)
            except UnicodeDecodeError:
                continue

        return value.decode("utf-8", errors="replace")

    def _is_text_type(self, sas_type: str) -> bool:
        """Determine if a SAS type represents a text/character column."""
        if not sas_type:
            return False

        type_lower = str(sas_type).lower().strip()
        return type_lower in ["s", "string", "c", "char", "character", "text"]

    def _is_numeric_type(self, sas_type: str) -> bool:
        """Determine if a SAS type represents a numeric column."""
        if not sas_type:
            return False

        type_lower = str(sas_type).lower().strip()
        return type_lower in ["d", "double", "n", "num", "numeric", "i", "int", "integer", "f", "float"]
