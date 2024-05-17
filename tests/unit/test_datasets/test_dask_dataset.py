from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset


def test_from_dict():
    data = {"test": ["A", "B", "C"]}

    dataset = DaskDataset.from_dict(data)
    assert "test" in dataset.data
