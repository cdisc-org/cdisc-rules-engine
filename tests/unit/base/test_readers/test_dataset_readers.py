from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyreadstat
import pytest
from pandas.io.sas.sas7bdat import SAS7BDATReader

from cdisc_rules_engine.readers.data_readers.base_data_reader import BaseDataReader
from cdisc_rules_engine.readers.data_readers.sas7_data_reader import Sas7DataReader
from cdisc_rules_engine.readers.data_readers.xpt_data_reader import XptDataReader

CLINICAL_DATA = Path(__file__).parents[3] / "resources" / "clinical_data"
XPT_FILES = sorted((CLINICAL_DATA / "xpt").glob("*/*.xpt"))
SAS7_FILES = sorted((CLINICAL_DATA / "sas7bdat").glob("*/*.sas7bdat"))


def _expected_frame(file_path: Path, file_format: str) -> pd.DataFrame:
    if file_format == "sas7bdat":
        with SAS7BDATReader(str(file_path), encoding=None, convert_dates=False) as reader:
            frame = reader.read()
    else:
        frame = pd.read_sas(str(file_path), format=file_format, encoding=None)
    for column in frame.columns:
        values = frame[column]
        if values.dtype == object:
            frame[column] = values.map(lambda value: "" if pd.isna(value) else BaseDataReader._decode(value))
        else:
            frame[column] = values.mask(values.abs() < 1e-20, 0.0)
    frame.columns = [column.lower() for column in frame.columns]
    return frame


def _read_all(reader) -> pd.DataFrame:
    metadata, chunks = reader.read()
    frames = [chunk.to_pandas() if isinstance(chunk, pa.Table) else chunk for chunk in chunks]
    if not frames:
        return pd.DataFrame(columns=[variable.name.lower() for variable in metadata.variables])
    return pd.concat(frames, ignore_index=True)


def _assert_same_values(actual: pd.DataFrame, expected: pd.DataFrame):
    assert list(actual.columns) == list(expected.columns)
    assert len(actual) == len(expected)
    for column in expected.columns:
        if expected[column].dtype == object:
            assert actual[column].tolist() == expected[column].tolist(), column
        else:
            np.testing.assert_array_equal(actual[column].to_numpy(dtype=float), expected[column].to_numpy(), column)


@pytest.mark.parametrize("file_path", XPT_FILES, ids=lambda path: path.name)
def test_xpt_reader_matches_pandas_across_chunks(file_path, monkeypatch):
    monkeypatch.setattr(XptDataReader, "CHUNK_BYTES", 2048)
    _assert_same_values(_read_all(XptDataReader(str(file_path))), _expected_frame(file_path, "xport"))


@pytest.mark.parametrize("file_path", SAS7_FILES, ids=lambda path: path.name)
def test_sas7_reader_matches_pandas(file_path):
    _assert_same_values(_read_all(Sas7DataReader(str(file_path))), _expected_frame(file_path, "sas7bdat"))


def test_xpt_reader_reports_metadata():
    file_path = XPT_FILES[0]
    reader = XptDataReader(str(file_path))
    metadata, _ = reader.read()

    assert metadata.full_path == str(file_path.resolve())
    assert metadata.file_size == file_path.stat().st_size
    assert metadata.modification_date
    assert reader._get_total_rows() == len(pd.read_sas(str(file_path), format="xport"))


@pytest.mark.parametrize("file_path", SAS7_FILES, ids=lambda path: path.name)
def test_sas7_reader_takes_labels_from_pyreadstat_and_formats_from_pandas(file_path):
    reader = Sas7DataReader(str(file_path))
    metadata, _ = reader.read()
    _, meta = pyreadstat.read_sas7bdat(str(file_path), metadataonly=True)
    pandas_reader = SAS7BDATReader(str(file_path), encoding=None)
    pandas_formats = [BaseDataReader._decode(column.format) for column in pandas_reader.columns]
    pandas_reader.close()

    assert [variable.label for variable in metadata.variables] == [
        meta.column_names_to_labels[variable.name] or "" for variable in metadata.variables
    ]
    assert [variable.format for variable in metadata.variables] == pandas_formats
    assert metadata.label == (meta.file_label or "")
    assert reader._get_total_rows() == meta.number_rows


def test_decode_column_keeps_text_values_alongside_bytes():
    reader = Sas7DataReader.__new__(Sas7DataReader)
    values = pd.Series([b"caf\xc3\xa9", "already text", None], dtype=object)
    assert reader._decode_column(values).tolist() == ["café", "already text", None]


def test_decode_column_falls_back_per_value_for_invalid_utf8():
    reader = Sas7DataReader.__new__(Sas7DataReader)
    values = pd.Series([b"plain", b"non\xa0breaking"], dtype=object)
    assert reader._decode_column(values).tolist() == ["plain", "non breaking"]
