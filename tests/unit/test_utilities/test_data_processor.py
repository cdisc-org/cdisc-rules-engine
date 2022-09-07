import json
import os
from typing import List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.utilities.data_processor import DataProcessor
from cdisc_rules_engine.utilities.utils import get_model_details_cache_key


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
def test_max_date(data, expected, operation_params: OperationParams):
    operation_params.dataframe = data
    operation_params.target = "dates"
    max_date = DataProcessor().max_date(operation_params)
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
def test_min_date(data, expected, operation_params: OperationParams):
    operation_params.dataframe = data
    operation_params.target = "dates"
    min_date = DataProcessor().min_date(operation_params)
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
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
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
    mock_data_service, target, expected_result, operation_params: OperationParams
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
    operation_params.target = target
    operation_params.datasets = datasets
    operation_params.dataset_path = dataset_path
    data_processor = DataProcessor(mock_data_service, InMemoryCacheService())
    result = data_processor.variable_value_count(operation_params)
    assert result == expected_result


@pytest.mark.parametrize(
    "target, standard, standard_version, expected_result",
    [({"STUDYID", "DOMAIN"}, "sdtmig", "3-1-2", {"STUDYID", "DOMAIN"})],
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
    mock_data_service,
    operation_params: OperationParams,
):
    file_path: str = (
        f"{os.path.dirname(__file__)}/../../resources/"
        f"mock_library_responses/get_sdtmig_response.json"
    )
    with open(file_path) as file:
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
    operation_params.target = target
    operation_params.datasets = datasets
    operation_params.dataset_path = dataset_path
    operation_params.standard = standard
    operation_params.standard_version = standard_version
    data_processor = DataProcessor(data_service=mock_data_service)
    assert data_processor.variable_names(operation_params) == expected_result


def test_valid_whodrug_references(
    installed_whodrug_dictionaries: dict, operation_params: OperationParams
):
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
    data_processor = DataProcessor(
        MagicMock(), installed_whodrug_dictionaries["cache_service"]
    )
    operation_params.dataframe = invalid_df
    operation_params.target = "AEINA"
    operation_params.domain = "AE"
    operation_params.whodrug_path = installed_whodrug_dictionaries["whodrug_path"]
    result = data_processor.valid_whodrug_references(operation_params)
    assert result.equals(pd.Series([True, True, False, False]))


def test_get_column_order_from_dataset(operation_params: OperationParams):
    """
    Unit test for DataProcessor.get_column_order_from_dataset.
    """
    operation_params.dataframe = pd.DataFrame.from_dict(
        {
            "STUDYID": [
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
            ],
            "AESEQ": [
                1,
                2,
            ],
            "USUBJID": [
                "TEST1",
                "TEST1",
            ],
        }
    )
    data_processor = DataProcessor()
    result: pd.Series = data_processor.get_column_order_from_dataset(operation_params)
    expected: pd.Series = pd.Series(
        [
            [
                "STUDYID",
                "DOMAIN",
                "AESEQ",
                "USUBJID",
            ],
            [
                "STUDYID",
                "DOMAIN",
                "AESEQ",
                "USUBJID",
            ],
        ]
    )
    assert result.equals(expected)


@pytest.mark.parametrize(
    "model_metadata",
    [
        {
            "datasets": [
                {
                    "name": "AE",
                    "datasetVariables": [
                        {
                            "name": "AETERM",
                            "ordinal": 4,
                        },
                        {
                            "name": "AESEQ",
                            "ordinal": 3,
                        },
                    ],
                }
            ],
            "classes": [
                {
                    "name": GENERAL_OBSERVATIONS_CLASS,
                    "classVariables": [
                        {
                            "name": "DOMAIN",
                            "role": VariableRoles.IDENTIFIER.value,
                            "ordinal": 2,
                        },
                        {
                            "name": "STUDYID",
                            "role": VariableRoles.IDENTIFIER.value,
                            "ordinal": 1,
                        },
                        {
                            "name": "TIMING_VAR",
                            "role": VariableRoles.TIMING.value,
                            "ordinal": 33,
                        },
                    ],
                },
            ],
        },
        {
            "classes": [
                {
                    "name": "Events",
                    "classVariables": [
                        {
                            "name": "AETERM",
                            "ordinal": 4,
                        },
                        {
                            "name": "AESEQ",
                            "ordinal": 3,
                        },
                    ],
                },
                {
                    "name": GENERAL_OBSERVATIONS_CLASS,
                    "classVariables": [
                        {
                            "name": "DOMAIN",
                            "role": VariableRoles.IDENTIFIER.value,
                            "ordinal": 2,
                        },
                        {
                            "name": "STUDYID",
                            "role": VariableRoles.IDENTIFIER.value,
                            "ordinal": 1,
                        },
                        {
                            "name": "TIMING_VAR",
                            "role": VariableRoles.TIMING.value,
                            "ordinal": 33,
                        },
                    ],
                },
            ],
        },
    ],
)
def test_get_column_order_from_library(
    operation_params: OperationParams, model_metadata: dict
):
    """
    Unit test for DataProcessor.get_column_order_from_library.
    Mocks cache call to return metadata.
    """
    operation_params.dataframe = pd.DataFrame.from_dict(
        {
            "STUDYID": [
                "TEST_STUDY",
                "TEST_STUDY",
                "TEST_STUDY",
            ],
            "AETERM": [
                "test",
                "test",
                "test",
            ],
        }
    )
    operation_params.domain = "AE"
    operation_params.standard = "sdtm"
    operation_params.standard_version = "3-4"

    # save model metadata to cache
    cache = InMemoryCacheService.get_instance()
    cache.add(
        get_model_details_cache_key(
            operation_params.standard, operation_params.standard_version
        ),
        model_metadata,
    )

    # execute operation
    data_service = LocalDataService.get_instance(cache_service=cache)
    data_processor = DataProcessor(data_service=data_service, cache=cache)
    result: pd.Series = data_processor.get_column_order_from_library(operation_params)
    variables: List[str] = [
        "STUDYID",
        "DOMAIN",
        "AESEQ",
        "AETERM",
        "TIMING_VAR",
    ]
    expected: pd.Series = pd.Series(
        [
            variables,
            variables,
            variables,
        ]
    )
    assert result.equals(expected)
