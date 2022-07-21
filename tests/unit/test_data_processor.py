import json
import os
from typing import List

from engine.models.dictionaries.whodrug import WhoDrugTermsFactory
from engine.services.cache.in_memory_cache_service import InMemoryCacheService
from engine.services.data_services import LocalDataService

from engine.utilities.data_processor import DataProcessor
import pandas as pd
import pytest
from engine.exceptions.custom_exceptions import InvalidMatchKeyError
from unittest.mock import patch, MagicMock


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict({"dates": ["2001-01-01", "", "2022-01-05"]}),
            pd.to_datetime("2022-01-05").isoformat(),
        ),
        (pd.DataFrame.from_dict({"dates": [None, None]}), ""),
    ],
)
def test_max_date(data, expected):
    max_date = DataProcessor.calc_max_date(data, "dates")
    assert max_date == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            pd.DataFrame.from_dict({"dates": ["2001-01-01", "", "2022-01-01"]}),
            pd.to_datetime("2001-01-01").isoformat(),
        ),
        (pd.DataFrame.from_dict({"dates": [None, None]}), ""),
    ],
)
def test_min_date(data, expected):
    min_date = DataProcessor.calc_min_date(data, "dates")
    assert min_date == expected


@pytest.mark.parametrize(
    "data",
    [
        (
            pd.DataFrame.from_dict(
                {
                    "RDOMAIN": ["AE", "EC", "EC", "AE"],
                    "IDVAR": ["AESEQ", "ECSEQ", "ECSEQ", "AESEQ"],
                    "IDVARVAL": [1, 2, 1, 3],
                }
            )
        ),
        (pd.DataFrame.from_dict({"RSUBJID": [1, 4, 6000]})),
    ],
)
def test_preprocess_relationship_dataset(data):
    datasets: List[dict] = [
        {
            "domain": "AE",
            "filename": "ae.xpt",
        },
        {
            "domain": "EC",
            "filename": "ec.xpt",
        },
        {
            "domain": "SUPP",
            "filename": "supp.xpt",
        },
        {
            "domain": "DM",
            "filename": "dm.xpt",
        },
    ]
    ae = pd.DataFrame.from_dict(
        {
            "AESTDY": [4, 5, 6],
            "STUDYID": [101, 201, 300],
            "AESEQ": [1, 2, 3],
        }
    )
    ec = pd.DataFrame.from_dict(
        {
            "ECSTDY": [500, 4],
            "STUDYID": [201, 101],
            "ECSEQ": [2, 1],
        }
    )
    dm = pd.DataFrame.from_dict({"USUBJID": [1, 2, 3, 4, 5, 6000]})
    path_to_dataset_map: dict = {
        "path/ae.xpt": ae,
        "path/ec.xpt": ec,
        "path/dm.xpt": dm,
        "path/data.xpt": data,
    }
    with patch(
        "engine.services.data_services.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        data_processor = DataProcessor(cache=InMemoryCacheService())
        reference_data = data_processor.preprocess_relationship_dataset(
            "path", data, datasets
        )
        if "IDVAR" in data:
            idvars = data["IDVAR"]
            domains = data["RDOMAIN"]
            for i, idvar in enumerate(idvars):
                assert idvar in reference_data[domains[i]]
        elif "RSUBJID" in data:
            assert "RSUBJID" in reference_data["DM"]
            assert pd.np.array_equal(reference_data["DM"]["RSUBJID"], dm["USUBJID"])


def test_filter_dataset_columns_by_metadata_and_rule():
    """
    Unit test for DataProcessor.filter_dataset_columns_by_metadata_and_rule function.
    """
    columns: List[str] = ["STUDYID", "DOMAIN", "AESEV", "AESER"]
    define_metadata: List[dict] = [
        {
            "define_variable_name": "AESEV",
            "define_variable_origin_type": "Collected",
        },
        {
            "define_variable_name": "AESER",
            "define_variable_origin_type": "Collected",
        },
    ]
    library_metadata: dict = {
        "STUDYID": {
            "core": "Exp",
        },
        "DOMAIN": {
            "core": "Exp",
        },
        "AESEV": {
            "core": "Perm",
        },
        "AESER": {
            "core": "Perm",
        },
        "AESEQ": {
            "core": "Exp",
        },
    }
    rule: dict = {
        "variable_origin_type": "Collected",
        "variable_core_status": "Perm",
    }
    filtered_columns: List[
        str
    ] = DataProcessor.filter_dataset_columns_by_metadata_and_rule(
        columns, define_metadata, library_metadata, rule
    )
    assert filtered_columns == [
        "AESEV",
        "AESER",
    ]


def test_merge_datasets_on_relationship_columns():
    """
    Unit test for DataProcessor.merge_datasets_on_relationship_columns method.
    """
    # prepare data
    left_dataset: pd.DataFrame = pd.DataFrame.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
            ],
            "AESEQ": [
                1,
                2,
                3,
            ],
        }
    )
    right_dataset: pd.DataFrame = pd.DataFrame.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "RDOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "QNAM": [
                "TEST",
                "TEST",
                "TEST",
                "TEST_1",
            ],
            "IDVAR": [
                "AESEQ",
                "AESEQ",
                "AESEQ",
                "AESEQ",
            ],
            "IDVARVAL": [
                "1.0",
                "2",
                "3.0",
                "3.0",
            ],
        }
    )

    # call the tested function and check the results
    merged_df: pd.DataFrame = DataProcessor.merge_datasets_on_relationship_columns(
        left_dataset=left_dataset,
        right_dataset=right_dataset,
        right_dataset_domain_name="SUPPAE",
        column_with_names="IDVAR",
        column_with_values="IDVARVAL",
    )
    expected_df: pd.DataFrame = pd.DataFrame.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "AESEQ": [
                1.0,
                2.0,
                3.0,
                3.0,
            ],
            "USUBJID.SUPPAE": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "RDOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "QNAM": [
                "TEST",
                "TEST",
                "TEST",
                "TEST_1",
            ],
            "IDVAR": [
                "AESEQ",
                "AESEQ",
                "AESEQ",
                "AESEQ",
            ],
            "IDVARVAL": [
                1.0,
                2.0,
                3.0,
                3.0,
            ],
        }
    )
    assert merged_df.equals(expected_df)


def test_merge_datasets_on_string_relationship_columns():
    """
    Unit test for DataProcessor.merge_datasets_on_relationship_columns method.
    Test the case when the columns that describe the relation
    are of a string type.
    """
    # prepare data
    left_dataset: pd.DataFrame = pd.DataFrame.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
            ],
            "AESEQ": [
                "CDISC_IA",
                "CDISC_IB",
                "CDISC_IC",
            ],
        }
    )
    right_dataset: pd.DataFrame = pd.DataFrame.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "RDOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "QNAM": [
                "TEST",
                "TEST",
                "TEST",
                "TEST_1",
            ],
            "IDVAR": [
                "AESEQ",
                "AESEQ",
                "AESEQ",
                "AESEQ",
            ],
            "IDVARVAL": [
                "CDISC_IA",
                "CDISC_IB",
                "CDISC_IC",
                "CDISC_IC",
            ],
        }
    )

    # call the tested function and check the results
    merged_df: pd.DataFrame = DataProcessor.merge_datasets_on_relationship_columns(
        left_dataset=left_dataset,
        right_dataset=right_dataset,
        right_dataset_domain_name="SUPPAE",
        column_with_names="IDVAR",
        column_with_values="IDVARVAL",
    )
    expected_df: pd.DataFrame = pd.DataFrame.from_dict(
        {
            "USUBJID": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "AESEQ": [
                "CDISC_IA",
                "CDISC_IB",
                "CDISC_IC",
                "CDISC_IC",
            ],
            "USUBJID.SUPPAE": [
                "CDISC01",
                "CDISC01",
                "CDISC01",
                "CDISC01",
            ],
            "RDOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "QNAM": [
                "TEST",
                "TEST",
                "TEST",
                "TEST_1",
            ],
            "IDVAR": [
                "AESEQ",
                "AESEQ",
                "AESEQ",
                "AESEQ",
            ],
            "IDVARVAL": [
                "CDISC_IA",
                "CDISC_IB",
                "CDISC_IC",
                "CDISC_IC",
            ],
        }
    )
    assert merged_df.equals(expected_df)


@pytest.mark.parametrize(
    "target, expected_result",
    [
        ("DOMAIN", {12: 2, 6: 2, 1: 2}),
        ("STUDYID", {4: 2, 7: 1, 9: 1, 8: 1, 12: 1}),
        ("AESEQ", {1: 1, 2: 1, 3: 1}),
        ("EXSEQ", {1: 1, 2: 1, 3: 1}),
        ("COOLVAR", {}),
    ],
)
def test_study_variable_value_occurrence_count(
    mock_data_service, target, expected_result
):
    dataset_path = "study/bundle/blah"
    datasets_map = {
        "AE": pd.DataFrame.from_dict(
            {"STUDYID": [4, 7, 9], "AESEQ": [1, 2, 3], "DOMAIN": [12, 6, 1]}
        ),
        "EX": pd.DataFrame.from_dict(
            {"STUDYID": [4, 8, 12], "EXSEQ": [1, 2, 3], "DOMAIN": [12, 6, 1]}
        ),
        "AE2": pd.DataFrame.from_dict(
            {"STUDYID": [4, 7, 9], "AESEQ": [1, 2, 3], "DOMAIN": [12, 6, 1]}
        ),
    }

    datasets = [
        {"domain": "AE", "filename": "AE"},
        {"domain": "EX", "filename": "EX"},
        {"domain": "AE", "filename": "AE2"},
    ]
    mock_data_service.get_dataset.side_effect = lambda name: datasets_map.get(
        name.split("/")[-1]
    )
    mock_data_service.join_split_datasets.side_effect = lambda func, files: pd.concat(
        [func(f) for f in files]
    )
    result = DataProcessor.study_variable_value_occurrence_count(
        target, datasets, dataset_path, mock_data_service, InMemoryCacheService()
    )
    assert result == expected_result


@pytest.mark.parametrize(
    "target, standard, standard_version, expected_result",
    [({"STUDYID", "DOMAIN"}, "sdtmig", "3-1-2", {"STUDYID", "DOMAIN"})],
)
@patch("engine.services.cdisc_library_service.CDISCLibraryClient.get_sdtmig")
def test_get_variable_names_for_given_standard(
    mock_get_sdtmig: MagicMock,
    target,
    standard,
    standard_version,
    expected_result,
    mock_data_service,
):
    with open(
        f"{os.path.dirname(__file__)}/../resources/mock_library_responses/get_sdtmig_response.json"
    ) as file:
        mock_sdtmig_details: dict = json.loads(file.read())
    mock_get_sdtmig.return_value = mock_sdtmig_details
    dataset_path = "study/bundle/blah"
    datasets_map = {
        "AE": pd.DataFrame.from_dict({"STUDYID": [4, 7, 9], "DOMAIN": [12, 6, 1]}),
        "EX": pd.DataFrame.from_dict({"STUDYID": [4, 8, 12], "DOMAIN": [12, 6, 1]}),
        "AE2": pd.DataFrame.from_dict({"STUDYID": [4, 7, 9], "DOMAIN": [12, 6, 1]}),
    }

    datasets = [
        {"domain": "AE", "filename": "AE"},
        {"domain": "EX", "filename": "EX"},
        {"domain": "AE", "filename": "AE2"},
    ]
    mock_data_service.get_dataset.side_effect = lambda name: datasets_map.get(
        name.split("/")[-1]
    )
    mock_data_service.join_split_datasets.side_effect = lambda func, files: pd.concat(
        [func(f) for f in files]
    )
    assert (
        DataProcessor.get_variable_names_for_given_standard(
            target,
            datasets,
            dataset_path,
            mock_data_service,
            standard=standard,
            standard_version=standard_version,
        )
        == expected_result
    )


def test_valid_whodrug_references(installed_whodrug_dictionaries: str):
    """
    Unit test for valid_whodrug_references function.
    """
    # create a dataset where 2 rows reference invalid terms
    invalid_df = pd.DataFrame.from_dict(
        {
            "DOMAIN": [
                "AE",
                "AE",
                "AE",
                "AE",
            ],
            "AEINA": ["A", "A01", "A01AC", "A01AD"],
        }
    )

    # call the operation and check result
    result = DataProcessor.valid_whodrug_references(
        invalid_df, "AEINA", "AE", dictionaries_path=installed_whodrug_dictionaries
    )
    assert result.equals(pd.Series([True, True, False, False]))
