from cdisc_rules_engine.operations.dataset_is_custom import DatasetIsCustom


class DummyLibraryMetadata:
    def __init__(self, model_metadata):
        self.model_metadata = model_metadata


class DummyDataset:
    def __init__(self, name, filename=None):
        self.name = name
        self.filename = filename


class DummyParams:
    def __init__(self, datasets, dataset_path):
        self.datasets = datasets
        self.dataset_path = dataset_path


def test_dataset_is_custom_false_for_supp_dataset_when_prefix_in_library():
    """SUPP domain is not treated as custom if the library contains any
    dataset with the same SUPP/SQ prefix."""
    model_metadata = {
        "datasets": [
            {"name": "DM"},
            {"name": "AE"},
            {"name": "SUPPQUAL"},
        ]
    }

    library_metadata = DummyLibraryMetadata(model_metadata)
    params = DummyParams(
        [
            DummyDataset("DM", "dm.xpt"),
            DummyDataset("AE", "ae.xpt"),
            DummyDataset("SUPPEC", "suppec.xpt"),
        ],
        "suppec.xpt",
    )
    op = DatasetIsCustom(
        params=params,
        library_metadata=library_metadata,
        original_dataset=None,
        cache_service=None,
        data_service=None,
    )

    assert op._execute_operation() is False


def test_dataset_is_custom_false_for_standard_dm_when_in_library():
    """Standard DM dataset is not treated as custom when its name exists in model_metadata."""
    model_metadata = {
        "datasets": [
            {"name": "DM"},
            {"name": "AE"},
        ]
    }
    library_metadata = DummyLibraryMetadata(model_metadata)
    params = DummyParams(
        [
            DummyDataset("DM", "dm.xpt"),
            DummyDataset("AE", "ae.xpt"),
        ],
        "dm.xpt",
    )
    op = DatasetIsCustom(
        params=params,
        library_metadata=library_metadata,
        original_dataset=None,
        cache_service=None,
        data_service=None,
    )

    assert op._execute_operation() is False


def test_dataset_is_custom_true_when_not_in_library():
    """Dataset is treated as custom when its name is not present in model metadata."""
    model_metadata = {
        "datasets": [
            {"name": "DM"},
            {"name": "AE"},
        ]
    }
    library_metadata = DummyLibraryMetadata(model_metadata)
    params = DummyParams(
        [
            DummyDataset("DM", "dm.xpt"),
            DummyDataset("XX", "xx.xpt"),
        ],
        "xx.xpt",
    )
    op = DatasetIsCustom(
        params=params,
        library_metadata=library_metadata,
        original_dataset=None,
        cache_service=None,
        data_service=None,
    )

    assert op._execute_operation() is True
