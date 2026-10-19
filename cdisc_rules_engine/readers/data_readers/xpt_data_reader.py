import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pandas.io.sas.sas_xport import (
    XportReader,
    _handle_truncated_float_vec,
    _parse_float_vec,
)

from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)
from cdisc_rules_engine.readers.data_readers.base_data_reader import BaseDataReader

_TRAILING_WHITESPACE = " \t\n\r\v\f"
_TRAILING_NUL = "\x00"


@dataclass(frozen=True)
class _XptLayout:
    dtype: np.dtype
    fields: List[Dict[str, Any]]
    names: List[str]
    record_start: int
    record_length: int
    nobs: int
    chunk_rows: int


class XptDataReader(BaseDataReader):
    """
    XPT Reader for datasets
    """

    FILE_EXTENSION = ".xpt"
    CHUNKSIZE = 50000
    CHUNK_BYTES = 8 * 1024 * 1024
    MAX_WORKERS = 4

    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._reader = None
        self._layout: Optional[_XptLayout] = None

    def read(self) -> Tuple[DatasetMetadata2, Iterable[pa.Table]]:
        self._reader = XportReader(self.file_path, encoding=None, chunksize=self.CHUNKSIZE)
        metadata = self._extract_metadata(self._reader)
        self._layout = self._build_layout(self._reader, metadata)
        self._reader.close()
        return metadata, self._read_tables()

    def _get_extension(self):
        return self.FILE_EXTENSION

    def _build_layout(self, reader: XportReader, metadata: DatasetMetadata2) -> _XptLayout:
        return _XptLayout(
            dtype=reader._dtype,
            fields=list(reader.fields),
            names=[variable.name.lower() for variable in metadata.variables],
            record_start=reader.record_start,
            record_length=reader.record_length,
            nobs=int(reader.nobs),
            chunk_rows=max(1, min(self.CHUNKSIZE, self.CHUNK_BYTES // max(reader.record_length, 1))),
        )

    def _read_tables(self) -> Iterable[pa.Table]:
        layout = self._layout
        if layout is None or layout.nobs <= 0:
            return

        workers = max(1, min(self.MAX_WORKERS, os.cpu_count() or 1))
        offsets = iter(range(0, layout.nobs, layout.chunk_rows))

        with ThreadPoolExecutor(workers, thread_name_prefix="xpt-parse") as pool:
            pending = deque()
            for offset in offsets:
                pending.append(pool.submit(self._parse_range, offset))
                if len(pending) > workers:
                    break

            while pending:
                table = pending.popleft().result()
                next_offset = next(offsets, None)
                if next_offset is not None:
                    pending.append(pool.submit(self._parse_range, next_offset))
                yield table

    def _parse_range(self, offset: int) -> pa.Table:
        layout = self._layout
        rows = min(layout.chunk_rows, layout.nobs - offset)

        with open(self.file_path, "rb") as handle:
            handle.seek(layout.record_start + offset * layout.record_length)
            raw = handle.read(rows * layout.record_length)

        records = np.frombuffer(raw, dtype=layout.dtype, count=rows)
        columns = []
        for index, field in enumerate(layout.fields):
            values = records["s" + str(index)]
            width = field["field_length"]
            if field["ntype"] == "numeric":
                columns.append(self._numeric_column(values, width))
            else:
                columns.append(self._char_column(values, width, rows))
        return pa.Table.from_arrays(columns, names=layout.names)

    @classmethod
    def _char_column(cls, values: np.ndarray, width: int, rows: int) -> pa.Array:
        buffer = pa.py_buffer(np.ascontiguousarray(values))
        fixed = pa.FixedSizeBinaryArray.from_buffers(pa.binary(width), rows, [None, buffer])
        try:
            decoded = fixed.cast(pa.string())
        except pa.ArrowInvalid:
            return cls._char_column_fallback(values)
        return pc.utf8_rtrim(pc.utf8_rtrim(decoded, characters=_TRAILING_NUL), characters=_TRAILING_WHITESPACE)

    @classmethod
    def _char_column_fallback(cls, values: np.ndarray) -> pa.Array:
        return pa.array([cls._decode(value.rstrip()) for value in values], type=pa.string())

    @staticmethod
    def _numeric_column(values: np.ndarray, width: int) -> pa.Array:
        padded = _handle_truncated_float_vec(values, width)
        missing = _missing_double(padded)
        parsed = _parse_float_vec(padded)
        parsed[missing] = np.nan
        np.copyto(parsed, 0.0, where=np.abs(parsed) < 1e-20)
        return pa.array(parsed, mask=np.isnan(parsed))

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

    def _extract_label(self, reader: XportReader) -> str:
        member = getattr(reader, "member_info", None) or {}
        return self._decode(member.get("label", "") or "").strip()

    def _get_total_rows(self, reader: XportReader = None) -> Optional[int]:
        if self._layout is not None:
            return self._layout.nobs
        reader = reader or self._reader
        if reader is None:
            return None
        try:
            return int(reader.nobs)
        except Exception:
            return None


def _missing_double(vec: np.ndarray) -> np.ndarray:
    """
    Which values are SAS missings rather than numbers.
    (This is basically identical to pandas' XPT reader fyi)
    """
    v = vec.view(dtype="u1,u1,u2,u4")
    miss = (v["f1"] == 0) & (v["f2"] == 0) & (v["f3"] == 0)
    miss &= ((v["f0"] >= 0x41) & (v["f0"] <= 0x5A)) | (v["f0"] == 0x5F) | (v["f0"] == 0x2E)
    return miss
