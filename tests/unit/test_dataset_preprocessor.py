from typing import List
from unittest.mock import MagicMock, patch
import os
import pandas as pd
import pytest

from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.utilities.dataset_preprocessor import DatasetPreprocessor
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.utilities import sdtm_utilities
from cdisc_rules_engine.config import ConfigService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

from cdisc_rules_engine.models.dataset import PandasDataset


def test_preprocess_no_datasets_in_rule(dataset_rule_equal_to_error_objects: dict):
    """
    Unit test for preprocess method. Checks the case when
    no datasets are provided in the rule.
    Expected behaviour is the original dataset returned.
    """
    dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "USUBJID": ["CDISC01", "CDISC01", "CDISC01"],
                "AESEQ": [
                    1,
                    2,
                    3,
                ],
            }
        )
    )
    datasets: List[dict] = [{"domain": "AE", "filename": "ae.xpt"}]
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        dataset, "AE", "path", data_service, InMemoryCacheService()
    )
    preprocessed_dataset: PandasDataset = preprocessor.preprocess(
        dataset_rule_equal_to_error_objects, datasets
    )
    assert preprocessed_dataset.data.equals(dataset.data)


@pytest.mark.parametrize(
    "join_type, expected_dataset",
    [
        (
            None,
            PandasDataset(
                pd.DataFrame.from_dict(
                    {
                        "ECSEQ": [
                            "1",
                            "2",
                        ],
                        "ECSTDY": [
                            4,
                            5,
                        ],
                        "STUDYID": [
                            "1",
                            "2",
                        ],
                        "USUBJID": [
                            "CDISC001",
                            "CDISC001",
                        ],
                        "AESEQ": [
                            "1",
                            "2",
                        ],
                        "AESTDY": [
                            4,
                            5,
                        ],
                        "TSSEQ": [
                            "1",
                            "2",
                        ],
                        "TSSTDY": [
                            31,
                            74,
                        ],
                    }
                )
            ),
        ),
        (
            "inner",
            PandasDataset(
                pd.DataFrame.from_dict(
                    {
                        "ECSEQ": [
                            "1",
                            "2",
                        ],
                        "ECSTDY": [
                            4,
                            5,
                        ],
                        "STUDYID": [
                            "1",
                            "2",
                        ],
                        "USUBJID": [
                            "CDISC001",
                            "CDISC001",
                        ],
                        "AESEQ": [
                            "1",
                            "2",
                        ],
                        "AESTDY": [
                            4,
                            5,
                        ],
                        "TSSEQ": [
                            "1",
                            "2",
                        ],
                        "TSSTDY": [
                            31,
                            74,
                        ],
                    }
                )
            ),
        ),
        (
            "left",
            PandasDataset(
                pd.DataFrame(
                    {
                        "ECSEQ": [
                            "1",
                            "2",
                            "3",
                            "4",
                            "5",
                        ],
                        "ECSTDY": [
                            4,
                            5,
                            6,
                            7,
                            8,
                        ],
                        "STUDYID": [
                            "1",
                            "2",
                            "1",
                            "2",
                            "3",
                        ],
                        "USUBJID": [
                            "CDISC001",
                            "CDISC001",
                            "CDISC002",
                            "CDISC002",
                            "CDISC003",
                        ],
                        "AESEQ": [
                            "1",
                            "2",
                            "3",
                            "4",
                            None,
                        ],
                        "AESTDY": pd.Series(
                            [
                                4,
                                5,
                                16,
                                17,
                                None,
                            ],
                            dtype="object",
                        ),
                        "_merge_AE": pd.Categorical(
                            [
                                "both",
                                "both",
                                "both",
                                "both",
                                "left_only",
                            ],
                            categories=["left_only", "right_only", "both"],
                            ordered=False,
                        ),
                        "TSSEQ": [
                            "1",
                            "2",
                            None,
                            None,
                            None,
                        ],
                        "TSSTDY": pd.Series(
                            [
                                31,
                                74,
                                None,
                                None,
                                None,
                            ],
                            dtype="object",
                        ),
                        "_merge_TS": pd.Categorical(
                            [
                                "both",
                                "both",
                                "left_only",
                                "left_only",
                                "left_only",
                            ],
                            categories=["left_only", "right_only", "both"],
                            ordered=False,
                        ),
                    }
                ),
            ),
        ),
    ],
)
@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_preprocess(
    mock_get_dataset: MagicMock,
    dataset_rule_equal_to: dict,
    join_type: str,
    expected_dataset: pd.DataFrame,
):
    """
    Unit test for preprocess method. Checks the case when
    we are merging 3 datasets. Expected behavior depends
    on join_type:
    - If None or "inner", expected behavior is a dataset
      with rows from all 3 datasets filtered by match keys.
    - If "left", expected behavior is a dataset with rows
    from the first dataset with rows added from the
    other 2 datasets when there are matching key values.
    """
    # create datasets
    ec_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "ECSEQ": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
                "ECSTDY": [
                    4,
                    5,
                    6,
                    7,
                    8,
                ],
                "STUDYID": [
                    "1",
                    "2",
                    "1",
                    "2",
                    "3",
                ],
                "USUBJID": [
                    "CDISC001",
                    "CDISC001",
                    "CDISC002",
                    "CDISC002",
                    "CDISC003",
                ],
            }
        )
    )
    ae_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESEQ": [
                    "1",
                    "2",
                    "3",
                    "4",
                ],
                "AESTDY": [
                    4,
                    5,
                    16,
                    17,
                ],
                "STUDYID": [
                    "1",
                    "2",
                    "1",
                    "2",
                ],
                "USUBJID": [
                    "CDISC001",
                    "CDISC001",
                    "CDISC002",
                    "CDISC002",
                ],
            }
        )
    )
    ts_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "TSSEQ": [
                    "1",
                    "2",
                ],
                "TSSTDY": [
                    31,
                    74,
                ],
                "STUDYID": [
                    "1",
                    "2",
                ],
                "USUBJID": [
                    "CDISC001",
                    "CDISC001",
                ],
            }
        )
    )

    # mock blob storage call
    path_to_dataset_map: dict = {
        os.path.join("path", "ae.xpt"): ae_dataset,
        os.path.join("path", "ts.xpt"): ts_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]

    # call preprocessor
    dataset_rule_equal_to["datasets"].append(
        {"domain_name": "TS", "match_key": ["STUDYID", "USUBJID"]}
    )

    if join_type:
        for ds in dataset_rule_equal_to["datasets"]:
            ds["join_type"] = join_type

    datasets: List[dict] = [
        {"domain": "AE", "filename": "ae.xpt"},
        {"domain": "TS", "filename": "ts.xpt"},
    ]

    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        ec_dataset,
        "EC",
        os.path.join("path", "ec.xpt"),
        data_service,
        InMemoryCacheService(),
    )
    preprocessed_dataset: pd.DataFrame = preprocessor.preprocess(
        dataset_rule_equal_to, datasets
    )
    assert preprocessed_dataset.data.equals(expected_dataset.data)


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_preprocess_relationship_dataset(
    mock_get_dataset: MagicMock, dataset_rule_record_in_parent_domain_equal_to: dict
):
    """
    Unit test for preprocess method. Checks the case when
    we are merging relationship datasets.
    """
    # create datasets
    ec_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "USUBJID": ["CDISC001", "CDISC005", "CDISC005", "CDISC005", "CDISC005"],
                "DOMAIN": [
                    "EC",
                    "AE",
                    "EC",
                    "EC",
                    "EC",
                ],
                "ECPRESP": [
                    "A",
                    "Y",
                    "Y",
                    "Y",
                    "B",
                ],
                "ECSEQ": [
                    1,
                    2,
                    3,
                    4,
                    5,
                ],
                "ECNUM": [
                    1,
                    2,
                    3,
                    4,
                    5,
                ],
            }
        )
    )
    suppec_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "USUBJID": [
                    "CDISC005",
                    "CDISC005",
                ],
                "RDOMAIN": [
                    "EC",
                    "EC",
                ],
                "QNAM": [
                    "ECREASOC",
                    "ECREASOS",
                ],
                "IDVAR": [
                    "ECSEQ",
                    "ECSEQ",
                ],
                "IDVARVAL": [
                    "4.0",
                    "5.0",
                ],
            }
        )
    )

    # mock blob storage call
    path_to_dataset_map: dict = {
        os.path.join("path", "ec.xpt"): ec_dataset,
        os.path.join("path", "suppec.xpt"): suppec_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]

    # call preprocessor
    datasets: List[dict] = [
        {
            "domain": "EC",
            "filename": "ec.xpt",
        },
        {
            "domain": "SUPPEC",
            "filename": "suppec.xpt",
        },
    ]

    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        ec_dataset,
        "EC",
        os.path.join("path", "ec.xpt"),
        data_service,
        InMemoryCacheService(),
    )
    preprocessed_dataset: pd.DataFrame = preprocessor.preprocess(
        dataset_rule_record_in_parent_domain_equal_to, datasets
    )
    expected_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "USUBJID": ["CDISC005", "CDISC005"],
                "DOMAIN": [
                    "EC",
                    "EC",
                ],
                "ECPRESP": [
                    "Y",
                    "B",
                ],
                "ECSEQ": [
                    4.0,
                    5.0,
                ],
                "ECNUM": [
                    4,
                    5,
                ],
                "RDOMAIN": [
                    "EC",
                    "EC",
                ],
                "QNAM": [
                    "ECREASOC",
                    "ECREASOS",
                ],
                "IDVAR": [
                    "ECSEQ",
                    "ECSEQ",
                ],
                "IDVARVAL": [
                    4.0,
                    5.0,
                ],
            }
        )
    )
    assert preprocessed_dataset.data.equals(expected_dataset.data)


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_preprocess_relrec_dataset(mock_get_dataset: MagicMock):
    """
    Unit test for preprocess method. Checks the case when
    we are merging datasets using relrec.
    """
    # Rule
    relrec_rule = {
        "core_id": "MockRule",
        "standards": [{"Name": "SDTMIG", "Version": "3.3"}],
        "classes": {"Include": [ALL_KEYWORD]},
        "domains": {"Include": ["EC"]},
        "datasets": [{"domain_name": "RELREC", "wildcard": "__", "match_key": []}],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {
                            "target": "ECSTDY",
                            "comparator": "RELREC.__STDY",
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Value of ECSTDY is equal to AESTDY.",
                },
            }
        ],
        "output_variables": [
            "ECSTDY",
        ],
    }
    # create datasets
    ec_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "ECSEQ": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
                "ECSTDY": [
                    4,
                    5,
                    6,
                    7,
                    8,
                ],
                "STUDYID": [
                    "1",
                    "2",
                    "1",
                    "2",
                    "3",
                ],
                "USUBJID": [
                    "CDISC001",
                    "CDISC001",
                    "CDISC002",
                    "CDISC002",
                    "CDISC003",
                ],
            }
        )
    )
    ae_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESEQ": [
                    "1",
                    "2",
                    "3",
                    "4",
                ],
                "AESTDY": [
                    4,
                    5,
                    16,
                    17,
                ],
                "STUDYID": [
                    "1",
                    "2",
                    "1",
                    "2",
                ],
                "USUBJID": [
                    "CDISC001",
                    "CDISC001",
                    "CDISC002",
                    "CDISC002",
                ],
            }
        )
    )
    relrec_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "RDOMAIN": [
                    "EC",
                    "AE",
                ],
                "IDVAR": [
                    "ECSEQ",
                    "AESEQ",
                ],
                "IDVARVAL": [
                    "",
                    "",
                ],
                "RELID": [
                    "ECAE",
                    "ECAE",
                ],
                "STUDYID": [
                    "1",
                    "1",
                ],
                "USUBJID": [
                    "",
                    "",
                ],
            }
        )
    )

    # mock blob storage call
    path_to_dataset_map: dict = {
        os.path.join("path", "ae.xpt"): ae_dataset,
        os.path.join("path", "relrec.xpt"): relrec_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]

    # call preprocessor
    datasets: List[dict] = [
        {"domain": "AE", "filename": "ae.xpt"},
        {"domain": "RELREC", "filename": "relrec.xpt"},
    ]

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    sdtm_utilities.get_all_model_wildcard_variables = MagicMock(
        return_value=["--SEQ", "--STDY"]
    )
    # execute operation
    data_service = LocalDataService.get_instance(
        cache_service=cache,
        config=ConfigService(),
    )
    data_service.library_metadata = LibraryMetadataContainer()

    preprocessor = DatasetPreprocessor(
        ec_dataset,
        "EC",
        os.path.join("path", "ec.xpt"),
        data_service,
        InMemoryCacheService(),
    )
    preprocessed_dataset: pd.DataFrame = preprocessor.preprocess(relrec_rule, datasets)
    expected_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "ECSEQ": ["1", "2", "3", "4"],
                "ECSTDY": [4, 5, 6, 7],
                "STUDYID": ["1", "2", "1", "2"],
                "USUBJID": [
                    "CDISC001",
                    "CDISC001",
                    "CDISC002",
                    "CDISC002",
                ],
                "RELREC.__SEQ": ["1", "2", "3", "4"],
                "RELREC.__STDY": [4, 5, 16, 17],
                "RELREC.STUDYID": ["1", "2", "1", "2"],
                "RELREC.USUBJID": [
                    "CDISC001",
                    "CDISC001",
                    "CDISC002",
                    "CDISC002",
                ],
            }
        )
    )
    assert preprocessed_dataset.data.equals(expected_dataset.data)


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_preprocess_with_merge_comparison(
    mock_get_dataset: MagicMock,
    dataset_rule_equal_to_compare_same_value: dict,
):
    """
    Unit test for the rules engine that ensures that
    the preprocess method correctly names variables from
    merged datasets.
    """
    target_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": [
                    "CDISCPILOT01",
                ],
                "DOMAIN": [
                    "IE",
                ],
                "USUBJID": [
                    "CDISC015",
                ],
                "NOTVISIT": [12],
            }
        )
    )
    match_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": [
                    "CDISCPILOT01",
                ],
                "DOMAIN": [
                    "AE",
                ],
                "USUBJID": [
                    "CDISC015",
                ],
                "VISIT": [24],
            }
        )
    )

    path_to_dataset_map: dict = {
        os.path.join("study_id", "data_bundle_id", "ae.xpt"): match_dataset,
        os.path.join("study_id", "data_bundle_id", "ec.xpt"): target_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]

    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        target_dataset,
        "EC",
        os.path.join("study_id", "data_bundle_id", "ec.xpt"),
        data_service,
        InMemoryCacheService(),
    )
    result: pd.DataFrame = preprocessor.preprocess(
        rule=dataset_rule_equal_to_compare_same_value,
        datasets=[
            {
                "domain": "AE",
                "filename": "ae.xpt",
            },
            {
                "domain": "EC",
                "filename": "ec.xpt",
            },
        ],
    )
    assert "NOTVISIT" in result
    assert result["NOTVISIT"].iloc[0] == 12
    assert "AE.VISIT" in result
    assert result["AE.VISIT"].iloc[0] == 24
