from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset


def test_from_dict():
    data = {"test": ["A", "B", "C"]}

    dataset = PandasDataset.from_dict(data)
    assert "test" in dataset.data
