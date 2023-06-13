from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.operations.variable_count import VariableCount
from cdisc_rules_engine.models.operation_params import OperationParams
import pandas as pd
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
import pytest
import os


@pytest.mark.parametrize(
    "target, expected",
    [("DOMAIN", 2), ("--SEQ", 2), ("SPECIALVAR", 1)],
)
def test_variable_count(
    target, expected, mock_data_service, operation_params: OperationParams
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    dataset_path = os.path.join("study", "bundle", "blah")
    datasets_map = {
        "AE": pd.DataFrame.from_dict(
            {"STUDYID": [4, 7, 9], "AESEQ": [1, 2, 3], "DOMAIN": [12, 6, 1]}
        ),
        "EX": pd.DataFrame.from_dict(
            {
                "STUDYID": [4, 8, 12],
                "EXSEQ": [1, 2, 3],
                "DOMAIN": [12, 6, 1],
                "SPECIALVAR": ["A", "B", "C"],
            }
        ),
        "AE2": pd.DataFrame.from_dict(
            {"STUDYID": [4, 7, 9], "AESEQ": [1, 2, 3], "DOMAIN": [12, 6, 1]}
        ),
        "RELREC": pd.DataFrame.from_dict({"LNKGRP": ["DOMAIN", "EXSEQ", "AESEQ"]}),
    }

    datasets = [
        {"domain": "AE", "filename": "AE"},
        {"domain": "EX", "filename": "EX"},
        {"domain": "AE", "filename": "AE2"},
    ]
    mock_data_service.get_dataset.side_effect = lambda name: datasets_map.get(
        os.path.split(name)[-1]
    )
    mock_data_service.join_split_datasets.side_effect = lambda func, files: pd.concat(
        [func(f) for f in files]
    )
    operation_params.datasets = datasets
    operation_params.target = target
    operation_params.original_target = target
    operation_params.dataset_path = dataset_path
    result = VariableCount(
        operation_params, datasets_map["AE"], cache, mock_data_service
    ).execute()
    assert operation_params.operation_id in result
    for val in result[operation_params.operation_id]:
        assert val == expected
