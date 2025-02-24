import os

from cdisc_rules_engine.services.data_readers.dataset_ndjson_reader import (
    DatasetNDJSONReader,
)


def test_from_file():
    test_dataset_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_dataset.ndjson"
    )

    reader = DatasetNDJSONReader()
    dataframe = reader.from_file(test_dataset_path)
    for value in dataframe["EXDOSE"]:
        """
        Verify that the rounding of incredibly small values to 0 is applied.
        """
        assert value == 0 or abs(value) > 10**-16
