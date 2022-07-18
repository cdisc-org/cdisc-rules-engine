from typing import List
from unittest.mock import patch, MagicMock

import pandas as pd
from engine.services.cache.in_memory_cache_service import InMemoryCacheService
from engine.services.local_data_service import LocalDataService

from engine.utilities.dataset_preprocessor import DatasetPreprocessor


def test_preprocess_no_datasets_in_rule(dataset_rule_equal_to_error_objects: dict):
    """
    Unit test for preprocess method. Checks the case when
    no datasets are provided in the rule.
    Expected behaviour is the original dataset returned.
    """
    dataset = pd.DataFrame.from_dict(
        {
            "USUBJID": ["CDISC01", "CDISC01", "CDISC01"],
            "AESEQ": [
                1,
                2,
                3,
            ],
        }
    )
    datasets: List[dict] = [{"domain": "AE", "filename": "ae.xpt"}]
    preprocessor = DatasetPreprocessor(
        dataset, "AE", "path", LocalDataService(), InMemoryCacheService()
    )
    preprocessed_dataset: pd.DataFrame = preprocessor.preprocess(
        dataset_rule_equal_to_error_objects, datasets
    )
    assert preprocessed_dataset.equals(dataset)


@patch("engine.services.local_data_service.LocalDataService.get_dataset")
def test_preprocess(mock_get_dataset: MagicMock, dataset_rule_equal_to: dict):
    """
    Unit test for preprocess method. Checks the case when
    we are merging 3 datasets. Expected behavior is a dataset
    with rows from all 3 datasets filtered by match keys.
    """
    # create datasets
    ec_dataset = pd.DataFrame.from_dict(
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
    ae_dataset = pd.DataFrame.from_dict(
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
    ts_dataset = pd.DataFrame.from_dict(
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

    # mock blob storage call
    path_to_dataset_map: dict = {
        "path/ae.xpt": ae_dataset,
        "path/ts.xpt": ts_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]

    # call preprocessor
    dataset_rule_equal_to["datasets"].append(
        {"domain_name": "TS", "match_key": ["STUDYID", "USUBJID"]}
    )
    datasets: List[dict] = [
        {"domain": "AE", "filename": "ae.xpt"},
        {"domain": "TS", "filename": "ts.xpt"},
    ]
    preprocessor = DatasetPreprocessor(
        ec_dataset, "EC", "path/ec.xpt", LocalDataService(), InMemoryCacheService()
    )
    preprocessed_dataset: pd.DataFrame = preprocessor.preprocess(
        dataset_rule_equal_to, datasets
    )
    expected_dataset = pd.DataFrame.from_dict(
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
    assert preprocessed_dataset.equals(expected_dataset)


@patch("engine.services.local_data_service.LocalDataService.get_dataset")
def test_preprocess_relationship_dataset(
    mock_get_dataset: MagicMock, dataset_rule_record_in_parent_domain_equal_to: dict
):
    """
    Unit test for preprocess method. Checks the case when
    we are merging relationship datasets.
    """
    # create datasets
    ec_dataset = pd.DataFrame.from_dict(
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
    suppec_dataset = pd.DataFrame.from_dict(
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

    # mock blob storage call
    path_to_dataset_map: dict = {
        "path/ec.xpt": ec_dataset,
        "path/suppec.xpt": suppec_dataset,
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
    preprocessor = DatasetPreprocessor(
        ec_dataset, "EC", "path/ec.xpt", LocalDataService(), InMemoryCacheService()
    )
    preprocessed_dataset: pd.DataFrame = preprocessor.preprocess(
        dataset_rule_record_in_parent_domain_equal_to, datasets
    )
    expected_dataset = pd.DataFrame.from_dict(
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
            "USUBJID.SUPPEC": ["CDISC005", "CDISC005"],
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
    assert preprocessed_dataset.equals(expected_dataset)


@patch("engine.services.local_data_service.LocalDataService.get_dataset")
def test_preprocess_with_merge_comparison(
    mock_get_dataset: MagicMock,
    dataset_rule_equal_to_compare_same_value: dict,
):
    """
    Unit test for the rules engine that ensures that
    the preprocess method correctly names variables from
    merged datasets.
    """
    target_dataset = pd.DataFrame.from_dict(
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
    match_dataset = pd.DataFrame.from_dict(
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

    path_to_dataset_map: dict = {
        "study_id/data_bundle_id/ae.xpt": match_dataset,
        "study_id/data_bundle_id/ec.xpt": target_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]
    preprocessor = DatasetPreprocessor(
        target_dataset,
        "EC",
        "study_id/data_bundle_id/ec.xpt",
        LocalDataService(),
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
