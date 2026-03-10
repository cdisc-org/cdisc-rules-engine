import os

import pytest

from cdisc_rules_engine.services.data_readers.xpt_reader import XPTReader
from cdisc_rules_engine.exceptions import UnsupportedXptFormatError


def test_read():
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.xpt"
    )
    with open(test_dataset_path, "rb") as f:
        data = f.read()

    reader = XPTReader()
    dataframe = reader.read(data)
    for value in dataframe["EXDOSE"]:
        """
        Verify that the rounding of incredibly small values to 0 is applied.
        """
        assert value == 0 or abs(value) > 10**-16


def test_read_xpt_v5_no_error():
    """Verify that an XPT v5 file can be read without errors."""
    test_dataset_path: str = os.path.join(
        os.path.dirname(__file__), "..", "resources", "test_dataset.xpt"
    )
    with open(test_dataset_path, "rb") as f:
        data = f.read()

    reader = XPTReader()
    df = reader.read(data)
    assert not df.empty


def test_read_xpt_v8_unsupported_error():
    """Verify that XPT v8 format raises UnsupportedXptFormatError."""
    test_dataset_path: str = os.path.join(
        os.path.dirname(__file__), "..", "resources", "test_dataset_sas_v8.xpt"
    )
    with open(test_dataset_path, "rb") as f:
        data = f.read()

    reader = XPTReader()
    expected_msg = (
        "Unsupported XPT (SAS Transport) format. Only Transport v5 is supported."
    )
    with pytest.raises(UnsupportedXptFormatError) as exc_info:
        reader.read(data)

    assert expected_msg in str(exc_info.value)
