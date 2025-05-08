from unittest.mock import Mock, patch, MagicMock
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

import pytest
import os
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services.data_services import cached_dataset, LocalDataService
from cdisc_rules_engine.utilities.utils import get_dataset_cache_key_from_path
from cdisc_rules_engine.constants.classes import (
    FINDINGS,
    FINDINGS_ABOUT,
    INTERVENTIONS,
    EVENTS,
    RELATIONSHIP,
    TRIAL_DESIGN,
    SPECIAL_PURPOSE,
    STUDY_REFERENCE,
)

from cdisc_rules_engine.models.validation_args import Validation_args
from scripts.script_utils import (
    get_library_metadata_from_cache,
)


@patch("cdisc_rules_engine.services.data_services.LocalDataService.read_metadata")
def test_get_dataset_metadata(mock_read_metadata: MagicMock, dataset_metadata: dict):
    # mock file read
    mock_read_metadata.return_value = dataset_metadata

    # mock cache service
    cache_mock = MagicMock()
    cache_mock.get = lambda cache_key: None

    data_service = LocalDataService(cache_mock, MagicMock(), MagicMock())
    actual_metadata: PandasDataset = data_service.get_dataset_metadata(
        dataset_name="dataset_name"
    )
    assert actual_metadata.equals(
        PandasDataset.from_dict(
            {
                "dataset_size": [dataset_metadata["file_metadata"]["file_size"]],
                "dataset_location": [dataset_metadata["file_metadata"]["name"]],
                "dataset_name": [dataset_metadata["contents_metadata"]["dataset_name"]],
                "dataset_label": [
                    dataset_metadata["contents_metadata"]["dataset_label"]
                ],
                "record_count": [
                    dataset_metadata["contents_metadata"]["dataset_length"]
                ],
            }
        )
    )


@patch("cdisc_rules_engine.services.data_services.LocalDataService.read_metadata")
def test_get_raw_dataset_metadata(
    mock_read_metadata: MagicMock, dataset_metadata: dict
):
    # mock file read
    mock_read_metadata.return_value = dataset_metadata

    # mock cache service
    cache_mock = MagicMock()
    cache_mock.get_dataset = lambda cache_key: None

    data_service = LocalDataService(cache_mock, MagicMock(), MagicMock())
    actual_metadata: SDTMDatasetMetadata = data_service.get_raw_dataset_metadata(
        dataset_name="dataset_name"
    )
    expected_metadata = SDTMDatasetMetadata(
        name=dataset_metadata["contents_metadata"]["dataset_name"],
        first_record=dataset_metadata["contents_metadata"]["first_record"],
        label=dataset_metadata["contents_metadata"]["dataset_label"],
        modification_date=dataset_metadata["contents_metadata"][
            "dataset_modification_date"
        ],
        filename=dataset_metadata["file_metadata"]["name"],
        full_path=dataset_metadata["file_metadata"]["path"],
        file_size=dataset_metadata["file_metadata"]["file_size"],
        record_count=20,
    )
    assert actual_metadata == expected_metadata


@pytest.mark.parametrize(
    "dataset_metadata, data, expected_class",
    [
        (
            {"name": "AE", "first_record": {"DOMAIN": "AE"}, "filename": "ae.xpt"},
            {"DOMAIN": ["AE"], "AETERM": ["test"]},
            EVENTS,
        ),
        (
            {"name": "CM", "first_record": {"DOMAIN": "CM"}, "filename": "cm.xpt"},
            {"DOMAIN": ["CM"], "CMTRT": ["test"]},
            INTERVENTIONS,
        ),
        (
            {"name": "VS", "first_record": {"DOMAIN": "VS"}, "filename": "vs.xpt"},
            {"DOMAIN": ["VS"], "VSTESTCD": ["test"]},
            FINDINGS,
        ),
        (
            {"name": "FA", "first_record": {"DOMAIN": "FA"}, "filename": "fa.xpt"},
            {"DOMAIN": ["FA"], "FATESTCD": ["test"], "FAOBJ": ["test"]},
            FINDINGS_ABOUT,
        ),
        (
            {"name": "FAMH", "first_record": {"DOMAIN": "FA"}, "filename": "famh.xpt"},
            {"DOMAIN": ["FA"], "FATESTCD": ["test"], "FAOBJ": ["test"]},
            FINDINGS_ABOUT,
        ),
        (
            {"name": "RELREC", "filename": "relrec.xpt"},
            {"RDOMAIN": ["AE"], "IDVAR": ["test"], "POOLID": ["test"]},
            RELATIONSHIP,
        ),
        (
            {"name": "SUPPAE", "filename": "suppae.xpt"},
            {"RDOMAIN": ["AE"], "IDVAR": ["test"], "QNAM": ["test"]},
            RELATIONSHIP,
        ),
        (
            {"name": "SQAPFAMH", "filename": "sqapfamh.xpt"},
            {"RDOMAIN": ["APFAMH"], "IDVAR": ["test"], "QNAM": ["test"]},
            RELATIONSHIP,
        ),
        (
            {"name": "OI", "first_record": {"DOMAIN": "OI"}, "filename": "oi.xpt"},
            {"DOMAIN": ["OI"], "OIPARMCD": ["test"], "OIPARM": ["test"]},
            STUDY_REFERENCE,
        ),
        (
            {"name": "TS", "first_record": {"DOMAIN": "TS"}, "filename": "ts.xpt"},
            {"DOMAIN": ["TS"], "TSPARMCD": ["test"], "TSPARM": ["test"]},
            TRIAL_DESIGN,
        ),
        (
            {"name": "XX", "first_record": {"DOMAIN": "XX"}, "filename": "xx.xpt"},
            {"DOMAIN": ["XX"], "XXOBJ": ["test"]},
            None,
        ),
        (
            {"name": "XY", "first_record": {"DOMAIN": "XY"}, "filename": "xy.xpt"},
            {"UNKNOWN": ["test"]},
            None,
        ),
        (
            {"name": "DM", "first_record": {"DOMAIN": "DM"}, "filename": "dm.xpt"},
            {"UNKNOWN": ["test"]},
            SPECIAL_PURPOSE,
        ),
        (
            {"name": "XX", "first_record": {"DOMAIN": "XX"}, "filename": "xx.xpt"},
            {"DOMAIN": ["XX"], "XXTRT": ["test"]},
            INTERVENTIONS,
        ),
        (
            {"name": "XX", "first_record": {"DOMAIN": "XX"}, "filename": "xx.xpt"},
            {"DOMAIN": ["XX"], "XXTESTCD": ["test"]},
            FINDINGS,
        ),
        (
            {"name": "XX", "first_record": {"DOMAIN": "XX"}, "filename": "xx.xpt"},
            {"DOMAIN": ["XX"], "XXTERM": ["test"]},
            EVENTS,
        ),
        # (
        #     {"name": "APDM", "first_record": {"DOMAIN": "APDM"}, "filename": "apdm.xpt"},
        #     {"DOMAIN": ["APDM"], "APID": ["001"]},
        #     SPECIAL_PURPOSE,
        # ),
    ],
)
def test_get_dataset_class(dataset_metadata, data, expected_class):
    df = PandasDataset.from_dict(data)
    mock_cache_service = MagicMock()
    library_metadata: LibraryMetadataContainer = get_library_metadata_from_cache(
        Validation_args(
            f"{os.path.dirname(__file__)}/../../../../resources/cache",
            10,
            [],
            "",
            "",
            "sdtmig",
            "3-4",
            None,
            "",
            "",
            "",
            False,
            "",
            None,
            "",
            "",
            None,
            "",
            None,
            False,
        )
    )
    data_service = LocalDataService(
        mock_cache_service,
        MagicMock(),
        MagicMock(),
        standard="sdtmig",
        standard_version="3-4",
        library_metadata=library_metadata,
    )
    class_name = data_service.get_dataset_class(
        df,
        dataset_metadata.get("filename"),
        [SDTMDatasetMetadata(**dataset_metadata)],
        SDTMDatasetMetadata(**dataset_metadata),
    )
    assert class_name == expected_class


def test_get_dataset_class_without_standard_and_version():
    df = PandasDataset.from_dict({"UNKNOWN": ["test"]})
    mock_cache_service = MagicMock()
    mock_cache_service.get.return_value = {
        "classes": [{"name": "SPECIAL PURPOSE", "datasets": [{"name": "DM"}]}]
    }
    data_service = LocalDataService(mock_cache_service, MagicMock(), MagicMock())
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "DM"}, filename="dm.xpt"
    )
    with pytest.raises(Exception):
        data_service.get_dataset_class(
            df, "dm.xpt", [dataset_metadata], dataset_metadata
        )


def test_get_dataset_class_associated_domains():
    datasets = [
        SDTMDatasetMetadata(**dataset)
        for dataset in [
            {"first_record": {"DOMAIN": "APDM"}, "filename": "apdm.xpt"},
            {"first_record": {"DOMAIN": "DM"}, "filename": "dm.xpt"},
        ]
    ]
    ap_dataset = PandasDataset.from_dict({"DOMAIN": ["APDM"], "APID": ["test"]})
    ce_dataset = PandasDataset.from_dict({"DOMAIN": ["DM"]})
    data_bundle_path = "cdisc/databundle"
    path_to_dataset_map: dict = {
        os.path.join(data_bundle_path, "apdm.xpt"): ap_dataset,
        os.path.join(data_bundle_path, "dm.xpt"): ce_dataset,
    }
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=ap_dataset,
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        library_metadata: LibraryMetadataContainer = get_library_metadata_from_cache(
            Validation_args(
                f"{os.path.dirname(__file__)}/../../../../resources/cache",
                10,
                [],
                "",
                "",
                "sdtmig",
                "3-4",
                None,
                "",
                "",
                "",
                False,
                "",
                None,
                "",
                "",
                None,
                "",
                None,
                False,
            )
        )
        data_service = LocalDataService(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            standard="sdtmig",
            standard_version="3-4",
            library_metadata=library_metadata,
        )
        filepath = f"{data_bundle_path}/apdm.xpt"
        class_name = data_service.get_dataset_class(
            ap_dataset,
            filepath,
            datasets,
            datasets[0],
        )
        assert class_name == SPECIAL_PURPOSE


def test_cached_data_cache_exists():
    """
    Unit test for cached_data decorator.
    Checks the case when cache contains the
    dataset, so the function should not be called
    and the cache data should be returned.
    """

    # create a test wrapped function
    @cached_dataset(DatasetTypes.CONTENTS.value)
    def to_be_decorated(instance, dataset_name: str):
        pass

    # mock cache get() method to return a dataset
    test_dataset_name: str = "CDISC01/test/ae.xpt"
    cache_key: str = get_dataset_cache_key_from_path(
        test_dataset_name, DatasetTypes.CONTENTS.value
    )
    test_cache_data: dict = {cache_key: PandasDataset()}
    instance_to_pass = Mock()
    instance_to_pass.cache_service.get_dataset = lambda x: test_cache_data[x]

    # ensure that cache data was returned
    result = to_be_decorated(instance_to_pass, dataset_name=test_dataset_name)
    assert result is test_cache_data[cache_key]


def test_cached_data_empty_cache():
    """
    Unit test for cached_data decorator.
    Checks the case when cache is empty, so
    a wrapped function has to be called.
    """
    test_dataset_name: str = "CDISC01/test/ae.xpt"
    test_df = PandasDataset.from_dict({"AETESTCD": [100]})

    # create a test wrapped function
    @cached_dataset(DatasetTypes.CONTENTS.value)
    def to_be_decorated(instance, dataset_name: str):
        mock_data = {test_dataset_name: test_df}
        return mock_data[dataset_name]

    # mock cache get() and add() methods
    instance_to_pass = Mock()
    instance_to_pass.cache_service.get_dataset = lambda x: None
    mock_db = {}
    instance_to_pass.cache_service.add_dataset = lambda key, dataset: mock_db.update(
        {key: dataset}
    )

    result = to_be_decorated(instance_to_pass, dataset_name=test_dataset_name)

    assert result.equals(test_df)
    assert mock_db == {
        get_dataset_cache_key_from_path(
            test_dataset_name, DatasetTypes.CONTENTS.value
        ): test_df
    }, "New dataset was not added to the cache"
