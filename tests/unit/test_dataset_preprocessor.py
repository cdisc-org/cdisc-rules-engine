from unittest.mock import MagicMock, patch
import os
import pandas as pd
import pytest

from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.utilities.dataset_preprocessor import DatasetPreprocessor
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
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
    datasets = [SDTMDatasetMetadata(name="AE")]
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        dataset,
        SDTMDatasetMetadata(name="AE", full_path="path"),
        data_service,
        InMemoryCacheService(),
    )
    preprocessed_dataset: PandasDataset = preprocessor.preprocess(
        dataset_rule_equal_to_error_objects, datasets
    )
    assert preprocessed_dataset.data.equals(dataset.data)


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_rdomain_supplemental_dataset_idvar_matching(mock_get_dataset: MagicMock):
    supp_data = {
        "USUBJID": ["CDISC001", "CDISC002"],
        "RDOMAIN": ["LB", "LB"],
        "IDVAR": ["LBSEQ", "LBSEQ"],
        "IDVARVAL": ["321", "456"],
        "QNAM": ["LBSPID", "LBSPID"],
        "QVAL": ["SP001", "SP002"],
    }
    lb_data = {
        "USUBJID": ["CDISC001", "CDISC002", "CDISC003"],
        "LBSEQ": [321.0, 456.0, 789.0],
        "LBTEST": ["Glucose", "Cholesterol", "Hemoglobin"],
        "LBSTRESN": [95.5, 180.2, 14.1],
    }
    supp_dataset = PandasDataset(pd.DataFrame(supp_data))
    lb_dataset = PandasDataset(pd.DataFrame(lb_data))
    lb_dataset.data["LBSEQ"] = lb_dataset.data["LBSEQ"].astype(object)
    mock_get_dataset.return_value = lb_dataset
    rule = {
        "core_id": "TestSupplementalIDVAR",
        "datasets": [
            {
                "domain_name": "SUPPLB",
                "child": True,
                "match_key": ["USUBJID", "IDVAR", "IDVARVAL"],
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "QVAL", "comparator": "test_value"},
                    }
                ]
            }
        ),
    }
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        supp_dataset,
        SDTMDatasetMetadata(
            name="SUPPLB",
            first_record={"RDOMAIN": "LB"},
            full_path=os.path.join("path", "supplb.xpt"),
        ),
        data_service,
        InMemoryCacheService(),
    )
    datasets = [
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "LB"},
            filename="lb.xpt",
        )
    ]
    result = preprocessor.preprocess(rule, datasets)
    assert len(result.data) == 2
    assert "USUBJID" in result.data.columns
    assert "QVAL" in result.data.columns
    assert "LBTEST" in result.data.columns
    matched_records = result.data[result.data["LBTEST"].notna()]
    assert len(matched_records) == 2
    assert "Glucose" in matched_records["LBTEST"].values
    assert "Cholesterol" in matched_records["LBTEST"].values


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_rdomain_integer_idvar_matching(mock_get_dataset: MagicMock):
    supp_data = {
        "USUBJID": ["CDISC001", "CDISC002"],
        "RDOMAIN": ["VS", "VS"],
        "IDVAR": ["VSSEQ", "VSSEQ"],
        "IDVARVAL": ["1", "2"],
        "QNAM": ["VSSPID", "VSSPID"],
        "QVAL": ["VITAL001", "VITAL002"],
    }
    vs_data = {
        "USUBJID": ["CDISC001", "CDISC002", "CDISC003"],
        "VSSEQ": [1, 2, 3],
        "VSTEST": ["Weight", "Height", "BMI"],
        "VSSTRESN": [70.5, 175.0, 23.0],
    }
    supp_dataset = PandasDataset(pd.DataFrame(supp_data))
    vs_dataset = PandasDataset(pd.DataFrame(vs_data))
    mock_get_dataset.return_value = vs_dataset
    rule = {
        "core_id": "TestIntegerIDVAR",
        "datasets": [
            {
                "domain_name": "SUPPVS",  # Match child dataset name
                "child": True,
                "match_key": ["USUBJID", "IDVAR", "IDVARVAL"],
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "QVAL", "comparator": "test"},
                    }
                ]
            }
        ),
    }
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        supp_dataset,
        SDTMDatasetMetadata(
            name="SUPPVS",
            first_record={"RDOMAIN": "VS"},
            full_path=os.path.join("path", "suppvs.xpt"),
        ),
        data_service,
        InMemoryCacheService(),
    )
    datasets = [
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "VS"},
            filename="vs.xpt",
        )
    ]
    result = preprocessor.preprocess(rule, datasets)
    assert len(result.data) == 2
    assert "QVAL" in result.data.columns
    assert "VSTEST" in result.data.columns
    matched_records = result.data[result.data["VSTEST"].notna()]
    assert len(matched_records) == 2
    assert "Weight" in matched_records["VSTEST"].values
    assert "Height" in matched_records["VSTEST"].values


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_rdomain_no_matches_found(mock_get_dataset: MagicMock):
    supp_data = {
        "USUBJID": ["CDISC001"],
        "RDOMAIN": ["AE"],
        "IDVAR": ["AESEQ"],
        "IDVARVAL": ["999"],
        "QNAM": ["AESPID"],
        "QVAL": ["AE999"],
    }
    ae_data = {
        "USUBJID": ["CDISC001", "CDISC001", "CDISC001"],
        "AESEQ": [1, 2, 3],
        "AETERM": ["Headache", "Nausea", "Dizziness"],
    }
    supp_dataset = PandasDataset(pd.DataFrame(supp_data))
    ae_dataset = PandasDataset(pd.DataFrame(ae_data))
    mock_get_dataset.return_value = ae_dataset
    rule = {
        "core_id": "TestNoMatches",
        "datasets": [
            {
                "domain_name": "SUPPAE",  # Match child dataset name
                "child": True,
                "match_key": ["USUBJID", "IDVAR", "IDVARVAL"],
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "QVAL", "comparator": "test"},
                    }
                ]
            }
        ),
    }
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        supp_dataset,
        SDTMDatasetMetadata(
            name="SUPPAE",
            first_record={"RDOMAIN": "AE"},
            full_path=os.path.join("path", "suppae.xpt"),
        ),
        data_service,
        InMemoryCacheService(),
    )
    datasets = [
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "AE"},
            filename="ae.xpt",
        )
    ]
    result = preprocessor.preprocess(rule, datasets)
    assert len(result.data) == 1
    assert result.data.iloc[0]["USUBJID"] == "CDISC001"
    assert result.data.iloc[0]["QVAL"] == "AE999"
    assert "AETERM" in result.data.columns
    assert pd.isna(result.data.iloc[0]["AETERM"])


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_rdomain_combined_standard_and_idvar_matching(mock_get_dataset: MagicMock):
    supp_data = {
        "USUBJID": ["CDISC001", "CDISC002", "CDISC003"],
        "STUDYID": ["STUDY01", "STUDY01", "STUDY01"],
        "RDOMAIN": ["LB", "LB", "LB"],
        "IDVAR": ["LBSEQ", "LBSEQ", "LBSEQ"],
        "IDVARVAL": ["1", "2", "3"],
        "QNAM": ["LBSPID", "LBSPID", "LBSPID"],
        "QVAL": ["LAB001", "LAB002", "LAB003"],
    }
    lb_data = {
        "USUBJID": ["CDISC001", "CDISC001", "CDISC002", "CDISC003"],
        "STUDYID": ["STUDY01", "STUDY01", "STUDY01", "STUDY01"],
        "LBSEQ": [1, 2, 2, 3],
        "LBTEST": ["Glucose", "Insulin", "Cholesterol", "Hemoglobin"],
        "LBSTRESN": [95.5, 12.3, 180.2, 14.1],
    }
    supp_dataset = PandasDataset(pd.DataFrame(supp_data))
    lb_dataset = PandasDataset(pd.DataFrame(lb_data))
    mock_get_dataset.return_value = lb_dataset
    rule = {
        "core_id": "TestCombinedMatching",
        "datasets": [
            {
                "domain_name": "SUPPLB",  # Match child dataset name
                "child": True,
                "match_key": ["STUDYID", "USUBJID", "IDVAR", "IDVARVAL"],
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "QVAL", "comparator": "test"},
                    }
                ]
            }
        ),
    }
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        supp_dataset,
        SDTMDatasetMetadata(
            name="SUPPLB",
            first_record={"RDOMAIN": "LB"},
            full_path=os.path.join("path", "supplb.xpt"),
        ),
        data_service,
        InMemoryCacheService(),
    )
    datasets = [
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "LB"},
            filename="lb.xpt",
        )
    ]
    result = preprocessor.preprocess(rule, datasets)
    assert len(result.data) == 3
    assert "LBTEST" in result.data.columns
    matched_records = result.data[result.data["LBTEST"].notna()]
    assert len(matched_records) == 3
    result_sorted = matched_records.sort_values(["USUBJID", "IDVARVAL"]).reset_index(
        drop=True
    )
    assert result_sorted.iloc[0]["USUBJID"] == "CDISC001"
    assert result_sorted.iloc[0]["LBTEST"] == "Glucose"
    assert result_sorted.iloc[1]["USUBJID"] == "CDISC002"
    assert result_sorted.iloc[1]["LBTEST"] == "Cholesterol"
    assert result_sorted.iloc[2]["USUBJID"] == "CDISC003"
    assert result_sorted.iloc[2]["LBTEST"] == "Hemoglobin"


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

    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        ec_dataset,
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "EC"}, full_path=os.path.join("path", "ec.xpt")
        ),
        data_service,
        InMemoryCacheService(),
    )
    preprocessed_dataset: pd.DataFrame = preprocessor.preprocess(
        dataset_rule_equal_to,
        [
            SDTMDatasetMetadata(first_record={"DOMAIN": "AE"}, filename="ae.xpt"),
            SDTMDatasetMetadata(first_record={"DOMAIN": "TS"}, filename="ts.xpt"),
        ],
    )
    assert preprocessed_dataset.data.equals(expected_dataset.data)


@pytest.mark.parametrize(
    "relrec, expected",
    [
        (
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
            },
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
            },
        ),
        (
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
                    "1",
                    "1",
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
            },
            {
                "ECSEQ": ["1"],
                "ECSTDY": [4],
                "STUDYID": ["1"],
                "USUBJID": [
                    "CDISC001",
                ],
                "RELREC.__SEQ": ["1"],
                "RELREC.__STDY": [4],
                "RELREC.STUDYID": ["1"],
                "RELREC.USUBJID": [
                    "CDISC001",
                ],
            },
        ),
    ],
)
@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_preprocess_relrec_dataset(
    mock_get_dataset: MagicMock, relrec: dict, expected: dict
):
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
    relrec_dataset = PandasDataset(pd.DataFrame.from_dict(relrec))

    # mock blob storage call
    path_to_dataset_map: dict = {
        os.path.join("path", "ae.xpt"): ae_dataset,
        os.path.join("path", "relrec.xpt"): relrec_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]

    # call preprocessor
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
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "EC"},
            full_path=os.path.join("path", "ec.xpt"),
        ),
        data_service,
        InMemoryCacheService(),
    )
    preprocessed_dataset: pd.DataFrame = preprocessor.preprocess(
        relrec_rule,
        [
            SDTMDatasetMetadata(first_record={"DOMAIN": "AE"}, filename="ae.xpt"),
            SDTMDatasetMetadata(name="RELREC", filename="relrec.xpt"),
        ],
    )
    expected_dataset = PandasDataset(pd.DataFrame.from_dict(expected))
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
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "EC"},
            full_path=os.path.join("study_id", "data_bundle_id", "ec.xpt"),
        ),
        data_service,
        InMemoryCacheService(),
    )
    result: pd.DataFrame = preprocessor.preprocess(
        rule=dataset_rule_equal_to_compare_same_value,
        datasets=[
            SDTMDatasetMetadata(first_record={"DOMAIN": "AE"}, filename="ae.xpt"),
            SDTMDatasetMetadata(first_record={"DOMAIN": "EC"}, filename="ec.xpt"),
        ],
    )
    assert "NOTVISIT" in result
    assert result["NOTVISIT"].iloc[0] == 12
    assert "AE.VISIT" in result
    assert result["AE.VISIT"].iloc[0] == 24


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_preprocess_supp_with_blank_idvar_idvarval(mock_get_dataset):
    """
    Test preprocessing when SUPP dataset has blank IDVAR and IDVARVAL.
    Should pivot and merge on static keys.
    """
    main_data = {
        "USUBJID": ["CDISC001", "CDISC002"],
        "DOMAIN": ["AE", "AE"],
        "AESEQ": [1, 2],
        "AETERM": ["Headache", "Nausea"],
    }
    main_dataset = PandasDataset(pd.DataFrame(main_data))
    supp_data = {
        "USUBJID": ["CDISC001", "CDISC002"],
        "RDOMAIN": ["AE", "AE"],
        "IDVAR": ["", ""],
        "IDVARVAL": ["", ""],
        "QNAM": ["AESPID", "AESPID"],
        "QVAL": ["SCREENING", "BASELINE"],
    }
    supp_dataset = PandasDataset(pd.DataFrame(supp_data))

    mock_get_dataset.return_value = supp_dataset
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        main_dataset,
        SDTMDatasetMetadata(first_record={"DOMAIN": "AE"}, full_path="path"),
        data_service,
        InMemoryCacheService(),
    )
    rule = {
        "core_id": "MockRule",
        "datasets": [
            {
                "domain_name": "SUPPAE",
                "match_key": ["USUBJID"],
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "AESPID", "comparator": "SCREENING"},
                    }
                ]
            }
        ),
    }
    datasets = [
        SDTMDatasetMetadata(
            name="SUPPAE", first_record={"RDOMAIN": "AE"}, filename="suppae.xpt"
        )
    ]
    result = preprocessor.preprocess(rule, datasets)
    assert len(result.data) == 2
    assert "AESPID" in result.data.columns
    assert "QNAM" not in result.data.columns
    assert "QVAL" not in result.data.columns
    assert (
        result.data[result.data["USUBJID"] == "CDISC001"]["AESPID"].values[0]
        == "SCREENING"
    )
    assert (
        result.data[result.data["USUBJID"] == "CDISC002"]["AESPID"].values[0]
        == "BASELINE"
    )


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_preprocess_supp_wildcard_matches_all_supp_datasets(
    mock_get_dataset: MagicMock,
):
    ae_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": ["CDISC-PILOT-01", "CDISC-PILOT-01"],
                "DOMAIN": ["AE", "AE"],
                "USUBJID": ["S001", "S001"],
                "AESEQ": [1, 2],
                "AETERM": ["Headache", "Nausea"],
                "AEDECOD": ["Headache", "Nausea"],
            }
        )
    )
    suppae_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": ["CDISC-PILOT-01", "CDISC-PILOT-01"],
                "RDOMAIN": ["AE", "AE"],
                "USUBJID": ["S001", "S001"],
                "IDVAR": ["AESEQ", "AESEQ"],
                "IDVARVAL": ["1", "2"],
                "QNAM": ["AESPID", "AESEV"],
                "QLABEL": ["Sponsor ID", "Severity"],
                "QVAL": ["SP001", "MILD"],
            }
        )
    )

    mock_get_dataset.return_value = suppae_dataset
    rule_with_supp_wildcard = {
        "core_id": "TestRule",
        "datasets": [
            {
                "domain_name": "SUPP--",
                "match_key": ["USUBJID"],
                "relationship_columns": {
                    "column_with_names": "QNAM",
                    "column_with_values": "QVAL",
                },
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "AESEV", "comparator": "MILD"},
                    }
                ]
            }
        ),
    }
    datasets = [
        SDTMDatasetMetadata(
            name="SUPPAE",
            first_record={"RDOMAIN": "AE"},
            filename="suppae.xpt",
        ),
    ]
    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        ae_dataset,
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "AE"}, full_path=os.path.join("path", "ae.xpt")
        ),
        data_service,
        InMemoryCacheService(),
    )

    result = preprocessor.preprocess(rule_with_supp_wildcard, datasets)
    assert len(result.data) == 2
    assert "RDOMAIN" in result.data.columns
    assert "AESPID" in result.data.columns
    assert "AESEV" in result.data.columns
    assert result.data.loc[0, "AESPID"] == "SP001"
    assert result.data.loc[1, "AESEV"] == "MILD"


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_preprocess_specific_suppae_dataset(
    mock_get_dataset: MagicMock,
):
    ae_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": ["CDISC-PILOT-01"],
                "DOMAIN": ["AE"],
                "USUBJID": ["S001"],
                "AESEQ": [1],
                "AETERM": ["Headache"],
            }
        )
    )
    suppae_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": ["CDISC-PILOT-01"],
                "RDOMAIN": ["AE"],
                "USUBJID": ["S001"],
                "IDVAR": [""],
                "IDVARVAL": [""],
                "QNAM": ["AESPID"],
                "QLABEL": ["Sponsor ID"],
                "QVAL": ["SP001"],
            }
        )
    )

    mock_get_dataset.return_value = suppae_dataset
    rule_with_specific_supp = {
        "core_id": "TestRule",
        "datasets": [
            {
                "domain_name": "SUPPAE",
                "match_key": ["USUBJID"],
            }
        ],
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "equal_to",
                        "value": {"target": "AESPID", "comparator": "SP001"},
                    }
                ]
            }
        ),
    }
    datasets = [
        SDTMDatasetMetadata(
            name="SUPPAE",
            first_record={"RDOMAIN": "AE"},
            filename="suppae.xpt",
        ),
    ]

    data_service = LocalDataService(MagicMock(), MagicMock(), MagicMock())
    preprocessor = DatasetPreprocessor(
        ae_dataset,
        SDTMDatasetMetadata(first_record={"DOMAIN": "AE"}, full_path="path"),
        data_service,
        InMemoryCacheService(),
    )

    result = preprocessor.preprocess(rule_with_specific_supp, datasets)

    assert len(result.data) == 1
    assert "AESPID" in result.data.columns
    assert result.data["AESPID"].values[0] == "SP001"
    assert "QNAM" not in result.data.columns
    assert "QVAL" not in result.data.columns


@pytest.fixture
def suppdm_with_race():
    suppdm_data = {
        "IDVAR": {0: "", 1: "", 2: ""},
        "IDVARVAL": {0: "", 1: "", 2: ""},
        "QEVAL": {0: "", 1: "", 2: ""},
        "QLABEL": {0: "Race 1", 1: "Race 2", 2: "Race 3"},
        "QNAM": {0: "RACE1", 1: "RACE2", 2: "RACE3"},
        "QORIG": {0: "CRF", 1: "CRF", 2: "CRF"},
        "QVAL": {0: "ASIAN", 1: "BLACK OR AFRICAN AMERICAN", 2: "WHITE"},
        "RACE1": {0: "ASIAN", 1: None, 2: None},
        "RACE2": {0: None, 1: "BLACK OR AFRICAN AMERICAN", 2: None},
        "RACE3": {0: None, 1: None, 2: "WHITE"},
        "RDOMAIN": {0: "DM", 1: "DM", 2: "DM"},
        "STUDYID": {0: "CDISCPILOT01", 1: "CDISCPILOT01", 2: "CDISCPILOT01"},
        "USUBJID": {0: "CDISC008", 1: "CDISC008", 2: "CDISC008"},
    }
    suppdm_df = PandasDataset(pd.DataFrame(suppdm_data))
    return suppdm_df


def test_data_processor_groups_qnam_suppdm_qvals(suppdm_with_race):
    assert suppdm_with_race.shape[0] == 3
    suppdm_df = DataProcessor().process_supp(suppdm_with_race).data
    # expected to group data
    assert suppdm_df.shape[0] == 1
    assert {"RACE1", "RACE2", "RACE3"}.issubset(set(suppdm_df.columns))
    assert suppdm_df.loc[0, ["RACE1", "RACE2", "RACE3"]].notna().all()


def test_dm_merged_with_suppdm_without_dupes(suppdm_with_race):
    dm = {
        "STUDYID": {7: "CDISCPILOT01"},
        "DOMAIN": {7: "DM"},
        "USUBJID": {7: "CDISC008"},
        "SUBJID": {7: "1445"},
    }

    assert dm.shape[0] == 3
