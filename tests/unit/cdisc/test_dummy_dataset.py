from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset


def test_valid_dataset_data():
    dataset_data = [
        {
            "filename": "ae.xpt",
            "label": "Adverse Events",
            "records": {"AESEQ": [1, 2, 3, 4], "DOMAIN": ["AE", "AE", "AE", "AE"]},
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
