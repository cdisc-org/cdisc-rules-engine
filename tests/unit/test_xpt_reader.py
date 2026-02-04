import os

from cdisc_rules_engine.services.data_readers.xpt_reader import XPTReader


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
