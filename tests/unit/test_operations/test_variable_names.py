from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.variable_names import VariableNames
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
from unittest.mock import MagicMock, patch
import os
import json


@pytest.mark.parametrize(
    "target, standard, standard_version, expected_result, dataset_type",
    [
        (
            {"STUDYID", "DOMAIN"},
            "sdtmig",
            "3-1-2",
            {"STUDYID", "DOMAIN"},
            PandasDataset,
        ),
        ({"STUDYID", "DOMAIN"}, "sdtmig", "3-1-2", {"STUDYID", "DOMAIN"}, DaskDataset),
    ],
)
@patch(
    "cdisc_rules_engine.services.cdisc_library_service.CDISCLibraryClient.get_sdtmig"
)
def test_get_variable_names_for_given_standard(
    mock_get_sdtmig: MagicMock,
    target,
    standard,
    standard_version,
    expected_result,
    dataset_type,
    mock_data_service,
    operation_params: OperationParams,
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    file_path: str = (
        f"{os.path.dirname(__file__)}/../../resources/"
        f"mock_library_responses/get_sdtmig_response.json"
    )
    with open(file_path) as file:
        mock_sdtmig_details: dict = json.loads(file.read())
    mock_get_sdtmig.return_value = mock_sdtmig_details
    dataset_path = "study/bundle/blah"
    datasets_map = {
        "AE": dataset_type.from_dict({"STUDYID": [4, 7, 9], "DOMAIN": [12, 6, 1]}),
        "EX": dataset_type.from_dict({"STUDYID": [4, 8, 12], "DOMAIN": [12, 6, 1]}),
        "AE2": dataset_type.from_dict({"STUDYID": [4, 7, 9], "DOMAIN": [12, 6, 1]}),
    }

    datasets = [
        {"domain": "AE", "filename": "AE"},
        {"domain": "EX", "filename": "EX"},
        {"domain": "AE", "filename": "AE2"},
    ]
    mock_data_service.get_dataset.side_effect = lambda name: datasets_map.get(
        name.split("/")[-1]
    )
    mock_data_service.concat_split_datasets.side_effect = lambda func, files: pd.concat(
        [func(f) for f in files]
    )
    operation_params.target = target
    operation_params.datasets = datasets
    operation_params.dataset_path = dataset_path
    operation_params.standard = standard
    operation_params.standard_version = standard_version
    result = VariableNames(
        operation_params, datasets_map["AE"], cache, data_service=mock_data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected_result
