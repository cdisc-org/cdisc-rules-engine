from engine.dummy_models.dummy_dataset import DummyDataset
from engine.exceptions.custom_exceptions import InvalidDatasetFormat
import pytest


def test_invalid_dataset_data():
    dataset_data = [
        {
            "domain2": "AE",
            "filename": "ae.xpt",
            "name": "AE",
            "records": {"AESEQ": [1, 2, 3, 4]},
        }
    ]
    with pytest.raises(InvalidDatasetFormat):
        DummyDataset(dataset_data)
