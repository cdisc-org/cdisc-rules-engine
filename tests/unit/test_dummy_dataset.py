import pytest

from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.exceptions.custom_exceptions import InvalidDatasetFormat


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


def test_valid_dataset_data():
    dataset_data = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
            "label": "Adverse Events",
            "records": {"AESEQ": [1, 2, 3, 4]},
        }
    ]
    dataset = DummyDataset(dataset_data[0])
    assert dataset.domain == "AE"


def test_get_dataset_metadata():
    dataset_data = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
            "label": "Adverse Events",
            "records": {"AESEQ": [1, 2, 3, 4]},
        }
    ]
    dataset = DummyDataset(dataset_data[0])
    metadata = dataset.get_metadata()
    assert "dataset_name" in metadata
    assert metadata["dataset_name"] == ["AE"]
    assert metadata["dataset_label"] == ["Adverse Events"]
