from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.operations.variable_library_metadata import (
    VariableLibraryMetadata,
)
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
import pytest

from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory


@pytest.mark.parametrize(
    "target, standard, standard_version, expected_result, dataset_type",
    [
        ("core", "sdtmig", "3-1-2", {"STUDYID": "Req", "DOMAIN": "Req"}, PandasDataset),
        ("core", "sdtmig", "3-1-2", {"STUDYID": "Req", "DOMAIN": "Req"}, DaskDataset),
    ],
)
def test_get_variable_metadata_for_given_standard(
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
            "DM": {
                "STUDYID": {"core": "Req", "ordinal": 1},
                "DOMAIN": {"core": "Req", "ordinal": 2},
            }
        },
    )
    dataset_path = "study/bundle/blah"
    datasets_map = {
        "DM": dataset_type.from_dict({"STUDYID": [4, 7, 9], "DOMAIN": [12, 6, 1]}),
        "EX": dataset_type.from_dict({"STUDYID": [4, 8, 12], "DOMAIN": [12, 6, 1]}),
        "DM2": dataset_type.from_dict({"STUDYID": [4, 7, 9], "DOMAIN": [12, 6, 1]}),
    }

    datasets = [
        {"first_record": {"DOMAIN": "DM"}, "filename": "DM"},
        {"first_record": {"DOMAIN": "EX"}, "filename": "EX"},
        {"first_record": {"DOMAIN": "DM"}, "filename": "DM2"},
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
    operation_params.domain = "DM"
    result = VariableLibraryMetadata(
        operation_params,
        datasets_map["DM"],
        cache,
        data_service=mock_data_service,
        library_metadata=library_metadata,
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected_result
