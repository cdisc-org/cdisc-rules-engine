from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.operations.variable_names import VariableNames
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory


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
def test_get_variable_names_for_given_standard(
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
    library_metadata = LibraryMetadataContainer(
        variables_metadata={
            "AE": {
                "STUDYID": {"name": "STUDYID", "core": "Req", "ordinal": 1},
                "DOMAIN": {"name": "DOMAIN", "core": "Req", "ordinal": 2},
            }
        },
    )
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
        operation_params,
        datasets_map["AE"],
        cache,
        data_service=mock_data_service,
        library_metadata=library_metadata,
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected_result
