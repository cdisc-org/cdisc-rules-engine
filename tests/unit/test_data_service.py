from typing import List
from unittest.mock import Mock, patch, MagicMock
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

import pandas as pd
import pytest
import os
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata
from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services.data_services import cached_dataset, LocalDataService
from cdisc_rules_engine.utilities.utils import get_dataset_cache_key_from_path
from cdisc_rules_engine.constants.classes import (
    FINDINGS,
    FINDINGS_ABOUT,
    INTERVENTIONS,
    EVENTS,
)


@patch("cdisc_rules_engine.services.data_services.LocalDataService.read_metadata")
def test_get_dataset_metadata(mock_read_metadata: MagicMock, dataset_metadata: dict):
    # mock file read
    mock_read_metadata.return_value = dataset_metadata

    # mock cache service
    cache_mock = MagicMock()
    cache_mock.get = lambda cache_key: None

    data_service = LocalDataService(cache_mock, MagicMock(), MagicMock())
    actual_metadata: pd.DataFrame = data_service.get_dataset_metadata(
        dataset_name="dataset_name"
    )
    assert actual_metadata.equals(
        pd.DataFrame.from_dict(
            {
                "dataset_size": [dataset_metadata["file_metadata"]["size"]],
                "dataset_location": [dataset_metadata["file_metadata"]["name"]],
                "dataset_name": [dataset_metadata["contents_metadata"]["dataset_name"]],
                "dataset_label": [
                    dataset_metadata["contents_metadata"]["dataset_label"]
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
    cache_mock.get = lambda cache_key: None

    data_service = LocalDataService(cache_mock, MagicMock(), MagicMock())
    actual_metadata: DatasetMetadata = data_service.get_raw_dataset_metadata(
        dataset_name="dataset_name"
    )
    expected_metadata = DatasetMetadata(
        name=dataset_metadata["contents_metadata"]["dataset_name"],
        domain_name=dataset_metadata["contents_metadata"]["domain_name"],
        label=dataset_metadata["contents_metadata"]["dataset_label"],
        modification_date=dataset_metadata["contents_metadata"][
            "dataset_modification_date"
        ],
        filename=dataset_metadata["file_metadata"]["name"],
        full_path=dataset_metadata["file_metadata"]["path"],
        size=dataset_metadata["file_metadata"]["size"],
        records=20,
    )
    assert actual_metadata == expected_metadata


@pytest.mark.parametrize(
    "datasets, data, expected_class, filename",
    [
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            {"DOMAIN": ["AE"], "AETERM": ["test"]},
            EVENTS,
            "ae.xpt",
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            {"DOMAIN": ["AE"], "AETRT": ["test"]},
            INTERVENTIONS,
            "ae.xpt",
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            {"DOMAIN": ["AE"], "AETESTCD": ["test"]},
            FINDINGS,
            "ae.xpt",
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            {"DOMAIN": ["AE"], "AETESTCD": ["test"], "AEOBJ": ["test"]},
            FINDINGS_ABOUT,
            "ae.xpt",
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            {"DOMAIN": ["AE"], "AEOBJ": ["test"]},
            None,
            "ae.xpt",
        ),
        ([{"domain": "AE", "filename": "ae.xpt"}], {"UNKNOWN": ["test"]}, None, "None"),
        (
            [{"domain": "DM", "filename": "dm.xpt"}],
            {"UNKNOWN": ["test"]},
            "SPECIAL PURPOSE",
            "dm.xpt",
        ),
    ],
)
def test_get_dataset_class(datasets, data, expected_class, filename):
    df = pd.DataFrame.from_dict(data)
    mock_cache_service = MagicMock()
    library_metadata = LibraryMetadataContainer(
        standard_metadata={
            "classes": [{"name": "SPECIAL PURPOSE", "datasets": [{"name": "DM"}]}]
        }
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
        df, filename, datasets, datasets[0].get("domain")
    )
    assert class_name == expected_class


def test_get_dataset_class_without_standard_and_version():
    df = pd.DataFrame.from_dict({"UNKNOWN": ["test"]})
    mock_cache_service = MagicMock()
    mock_cache_service.get.return_value = {
        "classes": [{"name": "SPECIAL PURPOSE", "datasets": [{"name": "DM"}]}]
    }
    data_service = LocalDataService(mock_cache_service, MagicMock(), MagicMock())
    with pytest.raises(Exception):
        data_service.get_dataset_class(
            df, "dm.xpt", [{"domain": "DM", "filename": "dm.xpt"}], "DM"
        )


def test_get_dataset_class_associated_domains():
    datasets: List[dict] = [
        {"domain": "APCE", "filename": "ap.xpt"},
        {"domain": "CE", "filename": "ce.xpt"},
    ]
    ap_dataset = pd.DataFrame.from_dict({"DOMAIN": ["APCE"]})
    ce_dataset = pd.DataFrame.from_dict({"DOMAIN": ["CE"], "CETERM": ["test"]})
    data_bundle_path = "cdisc/databundle"
    path_to_dataset_map: dict = {
        os.path.join(data_bundle_path, "ap.xpt"): ap_dataset,
        os.path.join(data_bundle_path, "ce.xpt"): ce_dataset,
    }
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=ap_dataset,
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
        filepath = f"{data_bundle_path}/ce.xpt"
        class_name = data_service.get_dataset_class(
            ap_dataset, filepath, datasets, "CE"
        )
        assert class_name == EVENTS


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
    test_cache_data: dict = {cache_key: pd.DataFrame()}
    instance_to_pass = Mock()
    instance_to_pass.cache_service.get = lambda x: test_cache_data[x]

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
    test_df = pd.DataFrame.from_dict({"AETESTCD": [100]})

    # create a test wrapped function
    @cached_dataset(DatasetTypes.CONTENTS.value)
    def to_be_decorated(instance, dataset_name: str):
        mock_data = {test_dataset_name: test_df}
        return mock_data[dataset_name]

    # mock cache get() and add() methods
    instance_to_pass = Mock()
    instance_to_pass.cache_service.get = lambda x: None
    mock_db = {}
    instance_to_pass.cache_service.add = lambda key, dataset: mock_db.update(
        {key: dataset}
    )

    result = to_be_decorated(instance_to_pass, dataset_name=test_dataset_name)

    assert result.equals(test_df)
    assert mock_db == {
        get_dataset_cache_key_from_path(
            test_dataset_name, DatasetTypes.CONTENTS.value
        ): test_df
    }, "New dataset was not added to the cache"
