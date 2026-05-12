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
    assert dataset.name == "AE"
    assert dataset.label == "Adverse Events"
