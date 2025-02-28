from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.variable_count import VariableCount
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
import pandas as pd
from cdisc_rules_engine.services.cache.cache_service_factory import CacheServiceFactory
import pytest
import os


@pytest.mark.parametrize(
    "target, expected, dataset_type",
    [
        ("DOMAIN", 2, PandasDataset),
        ("--SEQ", 2, DaskDataset),
        ("SPECIALVAR", 1, PandasDataset),
    ],
)
def test_variable_count(
    target, expected, mock_data_service, operation_params: OperationParams, dataset_type
):
    config = ConfigService()
    cache = CacheServiceFactory(config).get_cache_service()
    dataset_path = os.path.join("study", "bundle", "blah")
    datasets_map = {
        "AE": dataset_type.from_dict(
            {"STUDYID": [4, 7, 9], "AESEQ": [1, 2, 3], "DOMAIN": [12, 6, 1]}
        ),
        "EX": dataset_type.from_dict(
            {
                "STUDYID": [4, 8, 12],
                "EXSEQ": [1, 2, 3],
                "DOMAIN": [12, 6, 1],
                "SPECIALVAR": ["A", "B", "C"],
            }
        ),
        "AEXX": dataset_type.from_dict(
            {"STUDYID": [4, 7, 9], "AESEQ": [1, 2, 3], "DOMAIN": [12, 6, 1]}
        ),
        "RELREC": dataset_type.from_dict({"LNKGRP": ["DOMAIN", "EXSEQ", "AESEQ"]}),
    }

    datasets = [
        SDTMDatasetMetadata(**dataset)
        for dataset in [
            {
                "name": "AE",
                "first_record": {"DOMAIN": "AE"},
                "filename": "AE",
                "full_path": "AE",
            },
            {
                "name": "EX",
                "first_record": {"DOMAIN": "EX"},
                "filename": "EX",
                "full_path": "EX",
            },
            {
                "name": "AEXX",
                "first_record": {"DOMAIN": "AE"},
                "filename": "AEXX",
                "full_path": "AEXX",
            },
        ]
    ]
    mock_data_service.get_dataset.side_effect = (
        lambda *args, **kwargs: datasets_map.get(
            os.path.split(args[0] if args else kwargs.get("dataset_name"))[-1]
        )
    )
    mock_data_service.concat_split_datasets.side_effect = lambda func, files: pd.concat(
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
