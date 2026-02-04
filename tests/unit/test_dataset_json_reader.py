import os
import tempfile
import json

import pytest

from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.services.data_readers.dataset_json_reader import (
    DatasetJSONReader,
)
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidJSONFormat


def test_from_file():
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.json"
    )

    reader = DatasetJSONReader(PandasDataset)
    dataframe = reader.from_file(test_dataset_path)
    for value in dataframe["EXDOSE"]:
        """
        Verify that the rounding of incredibly small values to 0 is applied.
        """
        assert value == 0 or abs(value) > 10**-16


def test_read_json_file_fails_with_wrong_encoding():
    test_data = {
        "datasetJSONVersion": "1.1",
        "datasetJSONCreationDateTime": "2024-01-01T00:00:00",
        "sourceSystem": {"name": "Test", "version": "1.0"},
        "studyOID": "TEST.1",
        "metaDataVersionOID": "MDV.1",
        "itemGroupOID": "IG.TEST",
        "records": 1,
        "name": "TEST",
        "label": "Test Dataset",
        "columns": [
            {
                "itemOID": "IT.TEST.STUDYID",
                "name": "STUDYID",
                "label": "Study Identifier",
                "dataType": "string",
                "length": 10,
            }
        ],
        "rows": [["STUDY001"]],
    }
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as f:
        json_str = json.dumps(test_data, ensure_ascii=False)
        json_bytes = json_str.encode("cp1252").replace(
            b'"Test Dataset"', b'"Test\x92s Dataset"'
        )
        f.write(json_bytes)
        temp_path = f.name

    try:
        reader = DatasetJSONReader(PandasDataset, encoding="utf-8")
        with pytest.raises(InvalidJSONFormat):
            reader.read_json_file(temp_path)
    finally:
        os.unlink(temp_path)


def _minimal_dataset_json():
    """Minimal valid Dataset-JSON with non-ASCII character for encoding tests."""
    return {
        "datasetJSONVersion": "1.1",
        "datasetJSONCreationDateTime": "2024-01-01T00:00:00",
        "sourceSystem": {"name": "Test", "version": "1.0"},
        "studyOID": "TEST.1",
        "metaDataVersionOID": "MDV.1",
        "itemGroupOID": "IG.TEST",
        "records": 1,
        "name": "TEST",
        "label": "Test Dataset",
        "columns": [
            {
                "itemOID": "IT.TEST.STUDYID",
                "name": "STUDYID",
                "label": "Study Identifier",
                "dataType": "string",
                "length": 10,
            }
        ],
        "rows": [["STUDY001"]],
    }


@pytest.mark.parametrize(
    "encoding,label",
    [
        ("utf-8", "Test Dataset — utf-8"),
        ("utf-16", "Test Dataset — utf-16"),
        ("utf-32", "Test Dataset — utf-32"),
        ("cp1252", "Test Dataset"),
        ("latin-1", "Test Dataset latin-1 \xe9"),
    ],
)
def test_read_json_file_succeeds_with_encoding(encoding, label):
    """Test each encoding mentioned in README (utf-8, utf-16, utf-32, cp1252, latin-1)."""
    test_data = _minimal_dataset_json()
    test_data["label"] = label
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as f:
        f.write(json.dumps(test_data, ensure_ascii=False).encode(encoding))
        temp_path = f.name
    try:
        reader = DatasetJSONReader(PandasDataset, encoding=encoding)
        result = reader.read_json_file(temp_path)
        assert result["name"] == "TEST"
        assert result["label"] == label
    finally:
        os.unlink(temp_path)
