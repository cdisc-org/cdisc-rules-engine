from typing import List
from unittest.mock import Mock, patch, MagicMock

import pandas as pd
import pytest

from cdisc_rules_engine.models.dataset_types import DatasetTypes
from cdisc_rules_engine.services.data_services import cached_dataset, LocalDataService
from cdisc_rules_engine.utilities.utils import get_dataset_cache_key_from_path


@pytest.mark.parametrize(
    "dataset, data, expected_class, filename",
    [
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            {"AETERM": ["test"]},
            "Events",
            "ae.xpt",
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            {"AETRT": ["test"]},
            "Interventions",
            "ae.xpt",
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            {"AETESTCD": ["test"]},
            "Findings",
            "ae.xpt",
        ),
        ([{"domain": "AE", "filename": "ae.xpt"}], {"UNKNOWN": ["test"]}, None, "None"),
    ],
)
def test_get_dataset_class(dataset, data, expected_class, filename):
    dataset = pd.DataFrame.from_dict(data)
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    class_name = data_service.get_dataset_class(dataset, filename, dataset)
    assert class_name == expected_class


def test_get_dataset_class_associated_domains():
    datasets: List[dict] = [
        {"domain": "APCE", "filename": "ap.xpt"},
        {"domain": "CE", "filename": "ce.xpt"},
    ]
    ap_dataset = pd.DataFrame.from_dict({"DOMAIN": ["APCE"]})
    ce_dataset = pd.DataFrame.from_dict({"CETERM": ["test"]})
    data_bundle_path = "cdisc/databundle"
    path_to_dataset_map: dict = {
        f"{data_bundle_path}/ap.xpt": ap_dataset,
        f"{data_bundle_path}/ce.xpt": ce_dataset,
    }
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=ap_dataset,
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
        filepath = f"{data_bundle_path}/ce.xpt"
        class_name = data_service.get_dataset_class(ap_dataset, filepath, datasets)
        assert class_name == "Events"


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
