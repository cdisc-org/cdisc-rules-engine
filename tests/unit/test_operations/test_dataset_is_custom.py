import pytest

from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.operations.dataset_is_custom import DatasetIsCustom


class DummyDataset:
    def __init__(self, name, filename=None):
        self.name = name
        self.filename = filename


class DummyParams:
    def __init__(self, datasets, dataset_path):
        self.datasets = datasets
        self.dataset_path = dataset_path


@pytest.mark.parametrize(
    "description, model_datasets, study_datasets, dataset_path, expected",
    [
        (
            # SUPP domain is not treated as custom if the library contains any
            # dataset with the same SUPP/SQ prefix.
            "supp_dataset_not_custom_when_prefix_in_library",
            [
                {"name": "DM"},
                {"name": "AE"},
                {"name": "SUPPQUAL"},
            ],
            [
                DummyDataset("DM", "dm.xpt"),
                DummyDataset("AE", "ae.xpt"),
                DummyDataset("SUPPEC", "suppec.xpt"),
            ],
            "suppec.xpt",
            False,
        ),
        (
            # Standard DM dataset is not treated as custom when its name exists in model_metadata.
            "standard_dm_not_custom_when_in_library",
            [
                {"name": "DM"},
                {"name": "AE"},
            ],
            [
                DummyDataset("DM", "dm.xpt"),
                DummyDataset("AE", "ae.xpt"),
            ],
            "dm.xpt",
            False,
        ),
        (
            # Dataset is treated as custom when its name is not present in model metadata.
            "dataset_custom_when_not_in_library",
            [
                {"name": "DM"},
                {"name": "AE"},
            ],
            [
                DummyDataset("DM", "dm.xpt"),
                DummyDataset("XX", "xx.xpt"),
            ],
            "xx.xpt",
            True,
        ),
    ],
)
def test_dataset_is_custom(
    description, model_datasets, study_datasets, dataset_path, expected
):
    """Verify DatasetIsCustom for multiple scenarios using parametrization.

    Scenarios covered:
    - supplementary dataset with SUPP/SQ prefix present in the library;
    - standard DM dataset present in the library;
    - dataset not present in the library, which should be treated as custom.
    """

    model_metadata = {"datasets": model_datasets}
    library_metadata = LibraryMetadataContainer(model_metadata=model_metadata)
    params = DummyParams(study_datasets, dataset_path)

    op = DatasetIsCustom(
        params=params,
        library_metadata=library_metadata,
        original_dataset=None,
        cache_service=None,
        data_service=None,
    )

    assert op._execute_operation() is expected
