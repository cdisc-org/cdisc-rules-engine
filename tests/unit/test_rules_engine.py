import os
from typing import List
from unittest.mock import MagicMock, patch
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)

import pandas as pd
import pytest
from conftest import get_matches_regex_pattern_rule
from cdisc_rules_engine.constants.classes import GENERAL_OBSERVATIONS_CLASS
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.rules_engine import RulesEngine
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.models.external_dictionaries_container import (
    ExternalDictionariesContainer,
    DictionaryTypes,
)
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.models.dataset import PandasDataset


def test_get_schema():
    schema = RulesEngine().get_schema()
    assert "variables" in schema
    dataset_check_schema = {
        "name": "get_dataset",
        "label": "GET DATASET",
        "field_type": "dataframe",
        "options": [],
    }
    assert dataset_check_schema in schema["variables"]
    assert "actions" in schema
    generate_record_message_metadata = {
        "name": "generate_record_message",
        "label": "Generate Record Message",
        "params": [
            {"label": "Message", "name": "message", "fieldType": "text"},
            {"label": "Target", "name": "target", "fieldType": "text"},
        ],
    }
    generate_dataset_error_objects_metadata = {
        "name": "generate_dataset_error_objects",
        "label": "Generate Dataset Error Objects",
        "params": [
            {"label": "Message", "name": "message", "fieldType": "text"},
        ],
    }

    generate_single_error_metadata = {
        "name": "generate_single_error",
        "label": "Generate Single Error",
        "params": [
            {"label": "Message", "name": "message", "fieldType": "text"},
        ],
    }
    assert generate_record_message_metadata in schema["actions"]
    assert generate_dataset_error_objects_metadata in schema["actions"]
    assert generate_single_error_metadata in schema["actions"]


def test_validate_rule_invalid_suffix(
    mock_ae_record_rule_equal_to_suffix: dict,
):
    """
    Unit test for function validate_rule.
    Test the case when we are checking a string suffix.
    Dataset has 2 strings: valid and invalid.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESTDY": [
                    "valid-test",
                    "test-invalid",
                ],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            mock_ae_record_rule_equal_to_suffix,
            [],
            SDTMDatasetMetadata(
                name="AE",
                first_record={"DOMAIN": "AE"},
                filename="study/bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "bundle",
                "domain": "AE",
                "variables": ["AESTDY"],
                "message": "Suffix of AESTDY is equal to test.",
                "errors": [
                    {"value": {"AESTDY": "valid-test"}, "dataset": "bundle", "row": 1}
                ],
            }
        ]


def test_validate_rule_invalid_prefix(
    mock_record_rule_equal_to_string_prefix: dict,
):
    """
    Unit test for function validate_rule.
    Test the case when we are checking a string prefix.
    Dataset has 2 strings: valid and invalid.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESTDY": [
                    "test-valid",
                    "invalid-test",
                ],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            mock_record_rule_equal_to_string_prefix,
            [],
            SDTMDatasetMetadata(
                name="AE",
                first_record={"DOMAIN": "AE"},
                filename="bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "bundle",
                "domain": "AE",
                "variables": ["AESTDY"],
                "message": "Prefix of AESTDY is equal to test.",
                "errors": [
                    {"value": {"AESTDY": "test-valid"}, "dataset": "bundle", "row": 1}
                ],
            }
        ]


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_class")
def test_validate_rule_cross_dataset_check(
    mock_get_dataset_class, dataset_rule_equal_to: dict
):
    """
    The test checks that a rule can be executed for several datasets.
    We have 2 datasets that have common STUDYID and SUBJECTID columns and
    need to validate only that records from 1st dataset
    whose STUDYID and SUBJECTID present in the 2nd dataset.
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
                ],
                "ECSTDY": [
                    4,
                    5,
                    6,
                    7,
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
    mock_get_dataset_class.return_value = None
    # mock blob storage call
    path_to_dataset_map: dict = {
        os.path.join("path", "ae.xpt"): ae_dataset,
        os.path.join("path", "ec.xpt"): ec_dataset,
    }
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        datasets = [
            SDTMDatasetMetadata(
                name="EC",
                first_record={"DOMAIN": "EC"},
                filename="ec.xpt",
                full_path=os.path.join("path", "ec.xpt"),
            ),
            SDTMDatasetMetadata(
                name="AE",
                first_record={"DOMAIN": "AE"},
                filename="ae.xpt",
                full_path=os.path.join("path", "ae.xpt"),
            ),
        ]
        validation_result: List[str] = RulesEngine(
            standard="sdtmig", standard_version="3-4"
        ).validate_single_dataset(dataset_rule_equal_to, datasets, datasets[0])
        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "ec.xpt",
                "domain": "EC",
                "variables": ["ECSTDY"],
                "message": "Value of ECSTDY is equal to AESTDY.",
                "errors": [
                    {
                        "dataset": "ec.xpt",
                        "row": 1,
                        "value": {"ECSTDY": 4.0},
                        "USUBJID": "CDISC001",
                        "SEQ": 1,
                    },
                    {
                        "dataset": "ec.xpt",
                        "row": 2,
                        "value": {"ECSTDY": 5.0},
                        "USUBJID": "CDISC001",
                        "SEQ": 2,
                    },
                ],
            }
        ]


def test_validate_one_to_one_rel_across_datasets(dataset_rule_one_to_one_related: dict):
    """
    The test checks validation of one-to-one relationship
    across two datasets.
    """
    datasets = [
        SDTMDatasetMetadata(
            name="EC",
            first_record={"DOMAIN": "EC"},
            filename="ec.xpt",
            full_path=os.path.join("path", "ec.xpt"),
        ),
        SDTMDatasetMetadata(
            name="EC",
            first_record={"DOMAIN": "AE"},
            filename="ae.xpt",
            full_path=os.path.join("path", "ae.xpt"),
        ),
    ]
    ae_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": [
                    101,
                    201,
                    300,
                    101,
                ],
                "DOMAIN": [
                    "AE",
                    "DI",
                    "EC",
                    "AE",
                ],
                "VISITNUM": [
                    1,
                    2,
                    1,
                    3,
                ],
            }
        )
    )
    # this dataset violates one-to-one relationship and should flag an error
    ec_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": [
                    101,
                    201,
                    300,
                    101,
                ],
                "VISITNUM": [
                    1,
                    2,
                    1,
                    3,
                ],
                "VISIT": ["surgery", "treatment", "consulting", "consulting"],
            }
        )
    )
    path_to_dataset_map: dict = {
        os.path.join("path", "ae.xpt"): ae_dataset,
        os.path.join("path", "ec.xpt"): ec_dataset,
    }
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            dataset_rule_one_to_one_related,
            datasets,
            datasets[0],
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "ec.xpt",
                "domain": "EC",
                "variables": ["VISITNUM"],
                "message": "VISITNUM is not one-to-one related to VISIT",
                "errors": [
                    {"value": {"VISITNUM": 1}, "dataset": "ec.xpt", "row": 1},
                    {"value": {"VISITNUM": 1}, "dataset": "ec.xpt", "row": 3},
                    {"value": {"VISITNUM": 3}, "dataset": "ec.xpt", "row": 4},
                ],
            }
        ]


def test_validate_rule_single_dataset_check(dataset_rule_greater_than: dict):
    """
    The test checks the rules validation for a single dataset.
    In this case the rules does not have "datasets" key
    and datasets map is also empty.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "ECCOOLVAR": [20, 100, 10, 34],
                "AESTDY": [1, 2, 40, 50],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            dataset_rule_greater_than,
            [],
            SDTMDatasetMetadata(
                name="EC",
                first_record={"DOMAIN": "EC"},
                filename="bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "EC",
                "dataset": "bundle",
                "variables": ["ECCOOLVAR"],
                "message": "Value for ECCOOLVAR greater than 30.",
                "errors": [
                    {"value": {"ECCOOLVAR": 100}, "dataset": "bundle", "row": 2},
                    {"value": {"ECCOOLVAR": 34}, "dataset": "bundle", "row": 4},
                ],
            }
        ]


def test_validate_rule_equal_length(dataset_rule_has_equal_length: dict):
    """
    The test checks validation of column length.
    The case when rule needs to find records whose length is
    equal to a desired value.
    For example, check all ECCOOLVAR columns whose length is equal to 5.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "ECCOOLVAR": ["first_string", "equal"],
                "AESTDY": ["pokemon", "test"],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            dataset_rule_has_equal_length,
            [],
            SDTMDatasetMetadata(
                name="EC",
                first_record={"DOMAIN": "EC"},
                filename="bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "EC",
                "dataset": "bundle",
                "variables": ["ECCOOLVAR"],
                "message": "Length of ECCOOLVAR is equal to 5.",
                "errors": [
                    {"value": {"ECCOOLVAR": "equal"}, "dataset": "bundle", "row": 2}
                ],
            }
        ]


def test_validate_is_contained_by_distinct(mock_rule_distinct_operation: dict):
    datasets = [
        SDTMDatasetMetadata(
            name="DM",
            first_record={"DOMAIN": "DM"},
            filename="dm.xpt",
            full_path=os.path.join("path", "dm.xpt"),
        ),
        SDTMDatasetMetadata(
            name="AE",
            first_record={"DOMAIN": "AE"},
            filename="ae.xpt",
            full_path=os.path.join("path", "ae.xpt"),
        ),
    ]
    ae_dataset = PandasDataset(pd.DataFrame.from_dict({"AESTDY": [1, 2, 3, 5000]}))

    dm_dataset = PandasDataset(
        pd.DataFrame.from_dict({"USUBJID": [1, 2, 2, 3, 4, 5, 5, 3, 3, 3]})
    )

    path_to_dataset_map: dict = {
        os.path.join("path", "ae.xpt"): ae_dataset,
        os.path.join("path", "dm.xpt"): dm_dataset,
    }
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            mock_rule_distinct_operation,
            datasets,
            datasets[1],
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "ae.xpt",
                "domain": "AE",
                "variables": ["AESTDY"],
                "message": "Value for AESTDY not in DM.USUBJID",
                "errors": [{"value": {"AESTDY": 5000}, "dataset": "ae.xpt", "row": 4}],
            }
        ]


def test_validate_rule_not_equal_length(dataset_rule_has_not_equal_length: dict):
    """
    The test checks validation of column length.
    The case when rule needs to find records whose length is
    not equal to a desired value.
    For example, check all ECCOOLVAR columns whose length is not equal to 5.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "ECCOOLVAR": ["first_string", "valid"],
                "AESTDY": ["pokemon", "test"],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            dataset_rule_has_not_equal_length,
            [],
            SDTMDatasetMetadata(
                name="EC",
                first_record={"DOMAIN": "EC"},
                filename="bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "EC",
                "dataset": "bundle",
                "variables": ["ECCOOLVAR"],
                "message": "Length of ECCOOLVAR is not equal to 5.",
                "errors": [
                    {
                        "value": {"ECCOOLVAR": "first_string"},
                        "dataset": "bundle",
                        "row": 1,
                    }
                ],
            }
        ]


def test_validate_rule_multiple_conditions(dataset_rule_multiple_conditions: dict):
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "ECCOOLVAR": ["first_string", "valid", "cool"],
                "AESTDY": ["pokemon", "test", "item"],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            dataset_rule_multiple_conditions,
            [],
            SDTMDatasetMetadata(
                name="EC",
                first_record={"DOMAIN": "EC"},
                filename="bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "EC",
                "dataset": "bundle",
                "variables": ["ECCOOLVAR"],
                "message": (
                    "Length of ECCOOLVAR is not equal to 5 or ECCOOLVAR == cool."
                ),
                "errors": [
                    {"value": {"ECCOOLVAR": "valid"}, "dataset": "bundle", "row": 2},
                    {"value": {"ECCOOLVAR": "cool"}, "dataset": "bundle", "row": 3},
                ],
            }
        ]


def test_validate_record_rule_numbers_separated_by_dash_pattern():
    """
    The test checks matching "{number}-{number}" pattern.
    """
    number_number_pattern: str = r"^\d+\-\d+$"
    rule: dict = get_matches_regex_pattern_rule(number_number_pattern)
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict({"AESTDY": ["5-5", "10-10", "test"]})
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            rule,
            [],
            SDTMDatasetMetadata(
                name="AE",
                first_record={"DOMAIN": "AE"},
                filename="bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "bundle",
                "domain": "AE",
                "variables": ["AESTDY"],
                "message": "Records have the following pattern: ^\\d+\\-\\d+$",
                "errors": [
                    {"value": {"AESTDY": "5-5"}, "dataset": "bundle", "row": 1},
                    {"value": {"AESTDY": "10-10"}, "dataset": "bundle", "row": 2},
                ],
            }
        ]


def test_validate_record_rule_semi_colon_delimited_pattern():
    """
    The test checks matching semi-colon delimited pattern.
    """
    semi_colon_delimited_pattern: str = "[^,]*;[^,]*"
    rule: dict = get_matches_regex_pattern_rule(semi_colon_delimited_pattern)
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict({"AESTDY": ["5;5", "alex;alex", "test"]})
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            rule,
            [],
            SDTMDatasetMetadata(
                name="AE",
                first_record={"DOMAIN": "AE"},
                filename="bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "AE",
                "dataset": "bundle",
                "variables": ["AESTDY"],
                "message": "Records have the following pattern: [^,]*;[^,]*",
                "errors": [
                    {"value": {"AESTDY": "5;5"}, "dataset": "bundle", "row": 1},
                    {"value": {"AESTDY": "alex;alex"}, "dataset": "bundle", "row": 2},
                ],
            }
        ]


def test_validate_record_rule_no_letters_numbers_underscores():
    """
    The test checks that we can match a pattern like:
    A string contains characters other than letters, numbers or underscores.
    """
    # checks that string contains characters other than letters, numbers or underscores
    does_not_contain_pattern: str = "^((?![a-zA-Z0-9_]).)*$"
    rule: dict = get_matches_regex_pattern_rule(does_not_contain_pattern)
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict({"AESTDY": ["[.*)]#@", "alex", "|>.§!"]})
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            rule,
            [],
            SDTMDatasetMetadata(
                name="AE",
                first_record={"DOMAIN": "AE"},
                filename="bundle",
                full_path="study/bundle",
            ),
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "bundle",
                "domain": "AE",
                "variables": ["AESTDY"],
                "message": "Records have the following pattern: ^((?![a-zA-Z0-9_]).)*$",
                "errors": [
                    {"value": {"AESTDY": "[.*)]#@"}, "dataset": "bundle", "row": 1},
                    {"value": {"AESTDY": "|>.§!"}, "dataset": "bundle", "row": 3},
                ],
            }
        ]


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_metadata",
)
def test_validate_dataset_metadata(
    mock_get_dataset_metadata: MagicMock, dataset_metadata_not_equal_to_rule: dict
):
    """
    Unit test that checks dataset metadata validation.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "dataset_name": [
                    "AE",
                ],
                "dataset_size": [
                    5,
                ],
                "dataset_label": [
                    "Adverse Events",
                ],
            }
        )
    )
    mock_get_dataset_metadata.return_value = dataset_mock

    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        dataset_metadata_not_equal_to_rule,
        [],
        SDTMDatasetMetadata(
            name="EC",
            first_record={"DOMAIN": "EC"},
            filename="bundle",
            full_path="study/bundle",
        ),
    )
    assert validation_result == [
        {
            "domain": "EC",
            "dataset": "bundle",
            "errors": [],
            "executionStatus": "success",
            "message": None,
            "variables": [],
        }
    ]


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_metadata"
)
def test_validate_dataset_metadata_wrong_metadata(
    mock_get_dataset_metadata: MagicMock,
    dataset_metadata_not_equal_to_rule: dict,
):
    """
    Unit test that checks dataset metadata validation.
    Test the case when dataset contains the wrong data.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "dataset_name": [
                    "AD",
                ],
                "dataset_size": [
                    7,
                ],
                "dataset_label": [
                    "Events",
                ],
            }
        )
    )
    mock_get_dataset_metadata.return_value = dataset_mock

    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        dataset_metadata_not_equal_to_rule,
        [],
        SDTMDatasetMetadata(
            name="EC",
            first_record={"DOMAIN": "EC"},
            filename="bundle",
            full_path="study/bundle",
        ),
    )
    assert validation_result == [
        {
            "domain": "EC",
            "dataset": "bundle",
            "executionStatus": ExecutionStatus.SUCCESS.value,
            "variables": ["dataset_label", "dataset_name", "dataset_size"],
            "errors": [
                {
                    "dataset": "bundle",
                    "row": 1,
                    "value": {
                        "dataset_name": "AD",
                        "dataset_label": "Events",
                        "dataset_size": 7,
                    },
                }
            ],
            "message": "Dataset metadata is wrong.",
        }
    ]


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata",
)
def test_validate_variable_metadata(
    mock_get_variables_metadata: MagicMock, variables_metadata_rule: dict
):
    """
    Unit test that checks variable metadata validation.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "variable_name": ["STUDYID", "DOMAIN"],
                "variable_size": [5, 20],
                "variable_label": ["Study Identifier", "Domain Name"],
                "variable_data_type": ["Char", "Char"],
            }
        )
    )
    mock_get_variables_metadata.return_value = dataset_mock

    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        variables_metadata_rule,
        [],
        SDTMDatasetMetadata(
            name="EC",
            first_record={"DOMAIN": "EC"},
            full_path="study/bundle",
            filename="bundle",
        ),
    )
    assert validation_result == [
        {
            "domain": "EC",
            "dataset": "bundle",
            "errors": [],
            "executionStatus": "success",
            "message": None,
            "variables": [],
        }
    ]

    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        variables_metadata_rule,
        [],
        SDTMDatasetMetadata(
            name="EC",
            first_record={"DOMAIN": "EC"},
            full_path="study/bundle",
            filename="bundle",
        ),
    )
    assert validation_result == [
        {
            "domain": "EC",
            "dataset": "bundle",
            "errors": [],
            "executionStatus": "success",
            "message": None,
            "variables": [],
        }
    ]


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata",
)
def test_validate_variable_metadata_wrong_metadata(
    mock_get_variables_metadata: MagicMock, variables_metadata_rule: dict
):
    """
    Unit test that checks variable metadata validation.
    Test the case when variable metadata is wrong.
    """
    dataset_mock = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "variable_name": ["longer than eight", "longer than eight as well"],
                "variable_size": [5, 20],
                "variable_label": [
                    "Study Identifier Very Long Longer than 40",
                    "Long Long Label Very Long Longer than 40 chars",
                ],
                "variable_data_type": ["Char", "Char"],
            }
        )
    )
    mock_get_variables_metadata.return_value = dataset_mock

    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        variables_metadata_rule,
        [],
        SDTMDatasetMetadata(
            name="EC",
            first_record={"DOMAIN": "EC"},
            filename="bundle",
            full_path="study/bundle",
        ),
    )
    assert validation_result == [
        {
            "domain": "EC",
            "dataset": "bundle",
            "variables": ["variable_data_type", "variable_label", "variable_name"],
            "executionStatus": ExecutionStatus.SUCCESS.value,
            "errors": [
                {
                    "dataset": "bundle",
                    "row": 1,
                    "value": {
                        "variable_name": "longer than eight",
                        "variable_label": "Study Identifier Very Long Longer than 40",
                        "variable_data_type": "Char",
                    },
                },
                {
                    "dataset": "bundle",
                    "row": 2,
                    "value": {
                        "variable_name": "longer than eight as well",
                        "variable_label": (
                            "Long Long Label Very Long Longer than 40 chars"
                        ),
                        "variable_data_type": "Char",
                    },
                },
            ],
            "message": "Variable metadata is wrong.",
        }
    ]


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
)
def test_rule_with_domain_prefix_replacement(mock_get_dataset: MagicMock):
    rule = {
        "core_id": "TEST1",
        "standards": [],
        "domains": {"Include": [ALL_KEYWORD]},
        "conditions": ConditionCompositeFactory.get_condition_composite(
            {
                "all": [
                    {
                        "name": "get_dataset",
                        "operator": "greater_than",
                        "value": {
                            "target": "--STDY",
                            "comparator": 0,
                        },
                    }
                ]
            }
        ),
        "actions": [
            {
                "name": "generate_dataset_error_objects",
                "params": {
                    "message": "Invalid --STDY value",
                },
            }
        ],
    }
    df = PandasDataset(pd.DataFrame.from_dict({"AESTDY": [11, 12, 40, 59, 59]}))
    mock_get_dataset.return_value = df
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "AE"}, filename="bundle", full_path="study/bundle"
    )
    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(rule, [dataset_metadata], dataset_metadata)
    assert validation_result == [
        {
            "executionStatus": "success",
            "dataset": "bundle",
            "domain": "AE",
            "variables": ["AESTDY"],
            "message": "Invalid AESTDY value",
            "errors": [
                {"dataset": "bundle", "row": 1, "value": {"AESTDY": 11}},
                {"dataset": "bundle", "row": 2, "value": {"AESTDY": 12}},
                {"dataset": "bundle", "row": 3, "value": {"AESTDY": 40}},
                {"dataset": "bundle", "row": 4, "value": {"AESTDY": 59}},
                {"dataset": "bundle", "row": 5, "value": {"AESTDY": 59}},
            ],
        }
    ]


@pytest.mark.parametrize(
    "datasets, expected_validation_result",
    [
        (
            [
                "AE",
                "EC",
            ],
            [
                {
                    "executionStatus": "success",
                    "dataset": "bundle",
                    "domain": "AE",
                    "variables": ["AE"],
                    "message": "Domain AE exists",
                    "errors": [
                        {"value": {"AE": "ae.xpt"}, "dataset": "bundle", "row": 1}
                    ],
                }
            ],
        ),
        (
            [],
            [
                {
                    "domain": "AE",
                    "dataset": "bundle",
                    "errors": [],
                    "executionStatus": "success",
                    "message": None,
                    "variables": [],
                }
            ],
        ),
    ],
)
def test_validate_domain_presence(
    domain_presence_rule: dict, datasets: List[str], expected_validation_result: list
):
    """
    Unit test for RulesEngine.validate_domain_presence.
    """
    dataset_metadata = [
        SDTMDatasetMetadata(
            name=dataset,
            first_record={"DOMAIN": dataset},
            filename=f"{dataset.lower()}.xpt",
        )
        for dataset in datasets
    ]
    actual_validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        domain_presence_rule,
        dataset_metadata,
        SDTMDatasetMetadata(
            name="AE",
            first_record={"DOMAIN": "AE"},
            filename="bundle",
            full_path="study/bundle",
        ),
    )
    assert actual_validation_result == expected_validation_result


def test_validate_single_dataset(dataset_rule_equal_to_error_objects: dict):
    """
    Unit test for validate_single_dataset function.
    """
    df = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESTDY": ["test", "alex", "alex", "test", "test"],
                "USUBJID": [
                    1,
                    2,
                    2,
                    1,
                    3,
                ],
                "AESEQ": [
                    1,
                    2,
                    3,
                    4,
                    5,
                ],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=df,
    ):
        datasets = [
            SDTMDatasetMetadata(
                first_record={"DOMAIN": "AE"},
                filename="bundle",
                full_path="study/bundle",
            )
        ]
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            dataset_rule_equal_to_error_objects,
            datasets,
            datasets[0],
        )
        assert validation_result == [
            {
                "domain": "AE",
                "dataset": "bundle",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "variables": ["AESTDY"],
                "errors": [
                    {
                        "dataset": "bundle",
                        "row": 1,
                        "value": {
                            "AESTDY": "test",
                        },
                        "USUBJID": "1",
                        "SEQ": 1,
                    },
                    {
                        "dataset": "bundle",
                        "row": 4,
                        "value": {
                            "AESTDY": "test",
                        },
                        "USUBJID": "1",
                        "SEQ": 4,
                    },
                    {
                        "dataset": "bundle",
                        "row": 5,
                        "value": {
                            "AESTDY": "test",
                        },
                        "USUBJID": "3",
                        "SEQ": 5,
                    },
                ],
                "message": "Value of AESTDY is equal to test.",
            }
        ]


def test_validate_single_dataset_not_equal_to(
    dataset_rule_not_equal_to_error_objects: dict,
):
    """
    Unit test for validate_single_dataset function.
    Checks the case when all rule conditions are wrapped
    into "not" block.
    """
    df = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESTDY": ["test", "alex", "alex", "test", "test"],
                "USUBJID": [
                    1,
                    2,
                    2,
                    1,
                    3,
                ],
                "AESEQ": [
                    1,
                    2,
                    3,
                    4,
                    5,
                ],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=df,
    ):
        dataset_metadata = SDTMDatasetMetadata(
            first_record={"DOMAIN": "AE"},
            filename="data_bundle",
            full_path="study/data_bundle",
        )
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            dataset_rule_not_equal_to_error_objects,
            [dataset_metadata],
            dataset_metadata,
        )
        assert validation_result == [
            {
                "domain": "AE",
                "dataset": "data_bundle",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "variables": ["AESTDY"],
                "errors": [
                    {
                        "dataset": "data_bundle",
                        "row": 2,
                        "value": {
                            "AESTDY": "alex",
                        },
                        "USUBJID": "2",
                        "SEQ": 2,
                    },
                    {
                        "dataset": "data_bundle",
                        "row": 3,
                        "value": {
                            "AESTDY": "alex",
                        },
                        "USUBJID": "2",
                        "SEQ": 3,
                    },
                ],
                "message": RuleProcessor.extract_message_from_rule(
                    dataset_rule_not_equal_to_error_objects
                ),
            }
        ]


@pytest.mark.parametrize(
    "define_xml_metadata, dataset_mock, expected_validation_result",
    [
        (
            [
                {
                    "define_dataset_name": "AE",
                    "define_dataset_label": "Adverse Events",
                    "define_dataset_location": "ae.xpt",
                }
            ],
            PandasDataset(
                pd.DataFrame.from_dict(
                    {
                        "dataset_name": [
                            "AE",
                        ],
                        "dataset_label": [
                            "Adverse Events",
                        ],
                        "dataset_location": [
                            "te.xpt",
                        ],
                    }
                )
            ),
            [
                {
                    "executionStatus": "execution_error",
                    "dataset": "ae.xpt",
                    "domain": "AE",
                    "variables": [],
                    "message": "rule execution error",
                    "errors": [
                        {
                            "dataset": "ae.xpt",
                            "error": "An unknown exception has occurred",
                            "message": "single positional indexer is out-of-bounds",
                        }
                    ],
                }
            ],
        ),
        (
            [
                {
                    "define_dataset_name": "AE",
                    "define_dataset_label": "Adverse Events",
                    "define_dataset_location": "ae.xpt",
                }
            ],
            PandasDataset(
                pd.DataFrame.from_dict(
                    {
                        "dataset_name": [
                            "AE",
                        ],
                        "dataset_label": [
                            "Adverse Events",
                        ],
                        "dataset_location": [
                            "ae.xpt",
                        ],
                    }
                )
            ),
            [
                {
                    "domain": "AE",
                    "dataset": "ae.xpt",
                    "errors": [],
                    "executionStatus": "success",
                    "message": None,
                    "variables": [],
                }
            ],
        ),
    ],
)
@patch(
    "cdisc_rules_engine.dataset_builders.base_dataset_builder."
    + "BaseDatasetBuilder.get_define_metadata"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_metadata",
)
def test_validate_dataset_metadata_against_define_xml(
    mock_get_dataset_metadata: MagicMock,
    mock_get_define_xml_metadata_for_domain: MagicMock,
    define_xml_validation_rule: dict,
    define_xml_metadata: dict,
    dataset_mock: PandasDataset,
    expected_validation_result: List[dict],
):
    """
    Unit test for Define XML validation.
    Creates an invalid dataset and validates it against Define XML.
    """
    mock_get_define_xml_metadata_for_domain.return_value = define_xml_metadata
    mock_get_dataset_metadata.return_value = dataset_mock

    dataset_metadata = SDTMDatasetMetadata(
        name="AE",
        first_record={"DOMAIN": "AE"},
        full_path="CDISC01/test/ae.xpt",
        filename="ae.xpt",
    )
    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        define_xml_validation_rule,
        [dataset_metadata],
        dataset_metadata,
    )
    assert validation_result == expected_validation_result


@pytest.mark.parametrize(
    "variable_metadata, dataset_mock, expected_validation_result",
    [
        (
            [
                {
                    "define_variable_name": "TEST",
                    "define_variable_label": "TEST LABEL",
                    "define_variable_size": 20,
                    "define_variable_role": "VAR ROLE",
                    "define_variable_data_type": "Char",
                }
            ],
            PandasDataset(
                pd.DataFrame.from_dict(
                    {
                        "variable_name": [
                            "TEST",
                        ],
                        "variable_label": [
                            "TEST Label",
                        ],
                        "variable_size": [
                            30,
                        ],
                        "variable_role": ["VAR ROLE"],
                        "variable_data_type": ["Char"],
                    }
                )
            ),
            [
                {
                    "domain": "AE",
                    "dataset": "test",
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "variables": ["variable_size"],
                    "errors": [
                        {"dataset": "test", "row": 1, "value": {"variable_size": 30}}
                    ],
                    "message": (
                        "Variable metadata variable_size "
                        "does not match define variable size"
                    ),
                }
            ],
        ),
        (
            [
                {
                    "define_variable_name": "TEST2",
                    "define_variable_label": "TEST LABEL",
                    "define_variable_size": 20,
                    "define_variable_role": "VAR ROLE",
                    "define_variable_data_type": "Char",
                }
            ],
            PandasDataset(
                pd.DataFrame.from_dict(
                    {
                        "variable_name": [
                            "TEST",
                        ],
                        "variable_label": [
                            "TEST Label",
                        ],
                        "variable_size": [
                            30,
                        ],
                        "variable_role": ["VAR ROLE"],
                        "variable_data_type": ["Char"],
                    }
                )
            ),
            [
                {
                    "domain": "AE",
                    "dataset": "test",
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "variables": ["variable_size"],
                    "errors": [
                        {"dataset": "test", "row": 1, "value": {"variable_size": 30}}
                    ],
                    "message": (
                        "Variable metadata variable_size "
                        "does not match define variable size"
                    ),
                }
            ],
        ),
    ],
)
@patch(
    "cdisc_rules_engine.dataset_builders.base_dataset_builder."
    + "BaseDatasetBuilder.get_define_xml_variables_metadata"
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
def test_validate_variable_metadata_against_define_xml(
    mock_get_variables_metadata: MagicMock,
    mock_get_define_xml_variables_metadata: MagicMock,
    define_xml_variable_validation_rule: dict,
    variable_metadata: dict,
    dataset_mock: PandasDataset,
    expected_validation_result: List[dict],
):
    """
    Unit test for Define XML validation.
    Creates an invalid dataset and validates it against Define XML.
    """
    mock_get_define_xml_variables_metadata.return_value = variable_metadata
    mock_get_variables_metadata.return_value = dataset_mock
    dataset_metadata = SDTMDatasetMetadata(
        name="AE",
        first_record={"DOMAIN": "AE"},
        filename="test",
        full_path="CDISC01/test",
    )
    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        dataset_metadata=dataset_metadata,
        rule=define_xml_variable_validation_rule,
        datasets=[dataset_metadata],
    )
    assert validation_result == expected_validation_result


@patch(
    "cdisc_rules_engine.rules_engine.RulesEngine.get_define_xml_value_level_metadata"
)
def test_validate_value_level_metadata_against_define_xml(
    mock_get_define_xml_value_level_metadata,
    define_xml_value_level_metadata_validation_rule: dict,
):
    def check_length_func(row):
        return len(row["AETERM"]) < 10

    def filter_func(row):
        return row["FILTER"] == "SHORT"

    df = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "FILTER": ["LONG", "SHORT", "SHORT", "SHORT"],
                "AETERM": ["A" * 200, "A" * 200, "A" * 5, "A" * 15],
                "USUBJID": [
                    4,
                    5,
                    5,
                    5,
                ],
                "AESEQ": [
                    1,
                    2,
                    3,
                    4,
                ],
            }
        )
    )
    mock_get_define_xml_value_level_metadata.return_value = [
        {
            "define_variable_name": "AETERM",
            "filter": filter_func,
            "length_check": check_length_func,
        }
    ]
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=df,
    ):
        dataset_metadata = SDTMDatasetMetadata(
            first_record={"DOMAIN": "AE"}, filename="test", full_path="CDISC01/test"
        )
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig"
        ).validate_single_dataset(
            dataset_metadata=dataset_metadata,
            rule=define_xml_value_level_metadata_validation_rule,
            datasets=[dataset_metadata],
        )
        assert validation_result == [
            {
                "domain": "AE",
                "dataset": "test",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "variables": [
                    "AETERM",
                ],
                "errors": [
                    {
                        "dataset": "test",
                        "row": 2,
                        "value": {"AETERM": "A" * 200},
                        "USUBJID": "5",
                        "SEQ": 2,
                    },
                    {
                        "dataset": "test",
                        "row": 4,
                        "value": {"AETERM": "A" * 15},
                        "USUBJID": "5",
                        "SEQ": 4,
                    },
                ],
                "message": (
                    "Variable data does not match length specified "
                    "by value level metadata in define.xml"
                ),
            }
        ]


@pytest.mark.parametrize(
    "include_split_datasets, exclude, result",
    [
        (
            False,
            ["AE"],
            [
                {
                    "domain": "AE",
                    "dataset": "ae_2.xpt",
                    "executionStatus": ExecutionStatus.SKIPPED.value,
                    "variables": [],
                    "message": "Rule skipped - doesn't apply to domain for rule id=MockRule, dataset=",
                    "errors": [],
                }
            ],
        ),
        (
            True,
            [],
            [
                {
                    "domain": "AE",
                    "dataset": "ae_1.xpt, ae_2.xpt",
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "variables": ["AESTDY"],
                    "errors": [
                        {
                            "dataset": "ae_2.xpt",
                            "row": 1,
                            "value": {"AESTDY": "test"},
                            "USUBJID": "1",
                        },
                        {
                            "dataset": "ae_2.xpt",
                            "row": 4,
                            "value": {"AESTDY": "test"},
                            "USUBJID": "1",
                        },
                        {
                            "dataset": "ae_1.xpt",
                            "row": 4,
                            "value": {"AESTDY": "test"},
                            "USUBJID": "2",
                        },
                    ],
                    "message": "Value of AESTDY is equal to test.",
                }
            ],
        ),
    ],
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService._async_get_datasets",
)
def test_validate_split_dataset_contents(
    mock_async_get_datasets: MagicMock,
    dataset_rule_equal_to_error_objects: dict,
    include_split_datasets: bool,
    exclude: List[str],
    result: List[dict],
):
    """
    Unit test for validating contents of a split dataset.
    """
    dataset_rule_equal_to_error_objects["domains"][
        "include_split_datasets"
    ] = include_split_datasets

    dataset_rule_equal_to_error_objects["domains"]["Exclude"] = exclude

    # create two dataframes
    first_dataset_part: PandasDataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESTDY": [
                    "test",
                    "alex",
                    "50",
                    "test",
                ],
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "SEQ": [
                    1,
                    2,
                    3,
                    4,
                ],
            }
        )
    )
    second_dataset_part: PandasDataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AESTDY": [
                    "100",
                    "alex",
                    "Nic",
                    "test",
                ],
                "USUBJID": [
                    2,
                    2,
                    2,
                    2,
                ],
                "SEQ": [
                    1,
                    2,
                    3,
                    4,
                ],
            }
        )
    )

    # mock blob storage call and execute the validation
    mock_async_get_datasets.return_value = [first_dataset_part, second_dataset_part]
    datasets = [
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "AE"},
            filename="ae_2.xpt",
            full_path="CDISC01/test/ae_2.xpt",
        ),
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "AE"},
            filename="ae_1.xpt",
            full_path="CDISC01/test/ae_1.xpt",
        ),
    ]
    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        dataset_metadata=datasets[0],
        rule=dataset_rule_equal_to_error_objects,
        datasets=datasets,
    )
    # check validation result
    assert validation_result == result


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService._async_get_datasets",
)
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_metadata",
)
def test_validate_split_dataset_metadata(
    mock_get_dataset_metadata: MagicMock,
    mock_async_get_datasets: MagicMock,
    dataset_metadata_not_equal_to_rule: dict,
):
    """
    Unit test for validating metadata of a split dataset.
    """
    # create two dataframes
    first_dataset_part: PandasDataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "dataset_size": [
                    5,
                ],
                "dataset_location": [
                    "ec_2.xpt",
                ],
                "dataset_name": [
                    "EC",
                ],
                "dataset_label": [
                    "EC Label",
                ],
            }
        )
    )
    second_dataset_part: PandasDataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "dataset_size": [
                    10,
                ],
                "dataset_location": [
                    "ec_1.xpt",
                ],
                "dataset_name": [
                    "EC",
                ],
                "dataset_label": [
                    "EC Label",
                ],
            }
        )
    )

    # mock blob storage call and execute the validation
    mock_async_get_datasets.return_value = [first_dataset_part, second_dataset_part]
    mock_get_dataset_metadata.return_value = second_dataset_part
    datasets = [
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "EC"}, filename="ec_2.xpt", full_path="ec_2.xpt"
        ),
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "EC"}, filename="ec_1.xpt", full_path="ec_1.xpt"
        ),
    ]
    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        dataset_metadata=datasets[1],
        rule=dataset_metadata_not_equal_to_rule,
        datasets=datasets,
    )
    # check validation result.
    # error is contained only in the second part of the dataset.
    assert validation_result == [
        {
            "domain": "EC",
            "dataset": "ec_1.xpt",
            "executionStatus": ExecutionStatus.SUCCESS.value,
            "errors": [
                {
                    "dataset": "ec_1.xpt",
                    "row": 1,
                    "value": {
                        "dataset_label": "EC Label",
                        "dataset_name": "EC",
                        "dataset_size": 10,
                    },
                }
            ],
            "message": "Dataset metadata is wrong.",
            "variables": ["dataset_label", "dataset_name", "dataset_size"],
        }
    ]


@patch("cdisc_rules_engine.services.data_services.LocalDataService._async_get_datasets")
def test_validate_split_dataset_variables_metadata(
    mock_async_get_datasets: MagicMock, variables_metadata_rule: dict
):
    """
    Unit test for validating variables metadata of a split dataset.
    """
    first_dataset_part = PandasDataset(
        pd.DataFrame.from_dict(  # this part should flag an error
            {
                "variable_name": ["STUDYIDLONG", "DOMAINLONG"],
                "variable_size": [5, 20],
                "variable_label": [
                    "Study Identifier Study Identifier Very Long",
                    "Domain Name Domain Name Very Long",
                ],
                "variable_data_type": ["Char", "Char"],
            }
        )
    )
    second_dataset_part = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "variable_name": ["STUDYID", "DOMAIN"],
                "variable_size": [5, 20],
                "variable_label": ["Study Identifier", "Domain Name"],
                "variable_data_type": ["Char", "Char"],
            }
        )
    )

    mock_async_get_datasets.return_value = [
        first_dataset_part,
        second_dataset_part,
    ]
    datasets = [
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "EC"},
            filename="ec_2.xpt",
            full_path="CDISC/test/ec_2.xpt",
        ),
        SDTMDatasetMetadata(
            first_record={"DOMAIN": "EC"},
            filename="ec_1.xpt",
            full_path="CDISC/test/ec_1.xpt",
        ),
    ]
    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        rule=variables_metadata_rule,
        datasets=datasets,
        dataset_metadata=datasets[0],
    )
    assert validation_result == [
        {
            "domain": "EC",
            "dataset": "ec_2.xpt",
            "executionStatus": ExecutionStatus.SUCCESS.value,
            "variables": ["variable_data_type", "variable_label", "variable_name"],
            "errors": [
                {
                    "dataset": "ec_2.xpt",
                    "row": 1,
                    "value": {
                        "variable_label": (
                            "Study Identifier Study Identifier Very Long"
                        ),
                        "variable_name": "STUDYIDLONG",
                        "variable_data_type": "Char",
                    },
                }
            ],
            "message": "Variable metadata is wrong.",
        }
    ]


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_class")
def test_validate_record_in_parent_domain(
    mock_get_dataset_class,
    dataset_rule_record_in_parent_domain_equal_to: dict,
):
    """
    Unit test for validating value of a column in parent domain.
    """
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
    path_to_dataset_map: dict = {
        os.path.join("path", "ec.xpt"): ec_dataset,
        os.path.join("path", "suppec.xpt"): suppec_dataset,
    }
    mock_get_dataset_class.return_value = None
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        datasets = [
            SDTMDatasetMetadata(
                name="EC",
                first_record={"DOMAIN": "EC"},
                filename="ec.xpt",
                full_path=os.path.join("path", "ec.xpt"),
            ),
            SDTMDatasetMetadata(
                name="SUPPEC",
                first_record={"RDOMAIN": "EC"},
                filename="suppec.xpt",
                full_path=os.path.join("path", "suppec.xpt"),
            ),
        ]
        validation_result: List[str] = RulesEngine(
            standard="sdtmig", standard_version="3-4"
        ).validate_single_dataset(
            dataset_rule_record_in_parent_domain_equal_to,
            datasets,
            datasets[0],
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "EC",
                "dataset": "ec.xpt",
                "variables": ["ECPRESP", "QNAM"],
                "message": "Dataset contents is wrong.",
                "errors": [
                    {
                        "dataset": "ec.xpt",
                        "row": 4,
                        "value": {"ECPRESP": "Y", "QNAM": "ECREASOC"},
                        "USUBJID": "CDISC005",
                        "SEQ": 4,
                    }
                ],
            }
        ]


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_class")
def test_validate_additional_columns(
    mock_get_dataset_class, dataset_rule_inconsistent_enumerated_columns: dict
):
    """
    Unit test for validating additional columns like TSVAL1, TSVAL2.
    """
    mock_get_dataset_class.return_value = None
    dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "USUBJID": [
                    1,
                    1,
                    1,
                    1,
                ],
                "TSVAL": [
                    "value",
                    None,
                    "another value",
                    None,
                ],  # original column may be empty
                "TSVAL1": ["value", None, "value", "value"],  # invalid column
                "TSVAL2": ["value 2", "value 2", "value 2", None],  # invalid column
                "TSVAL3": ["value 3", "value 3", None, "value 3"],
            }
        )
    )
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset,
    ):
        datset_metadata = SDTMDatasetMetadata(
            first_record={"DOMAIN": "TS"},
            filename="ts.xpt",
            full_path="CDISC01/test/ts.xpt",
        )
        validation_result: List[dict] = RulesEngine(
            standard="sdtmig", standard_version="3-4"
        ).validate_single_dataset(
            rule=dataset_rule_inconsistent_enumerated_columns,
            datasets=[datset_metadata],
            dataset_metadata=datset_metadata,
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "dataset": "ts.xpt",
                "domain": "TS",
                "variables": ["TSVAL"],
                "message": "Inconsistencies found in enumerated TSVAL columns.",
                "errors": [
                    {
                        "value": {"TSVAL": "null"},
                        "dataset": "ts.xpt",
                        "row": 2,
                        "USUBJID": "1",
                    },
                    {
                        "value": {"TSVAL": "null"},
                        "dataset": "ts.xpt",
                        "row": 4,
                        "USUBJID": "1",
                    },
                ],
            }
        ]


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_define_xml_contents"
)
@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_class")
def test_validate_dataset_contents_against_define_and_library_variable_metadata(
    mock_get_dataset_class: MagicMock,
    mock_get_dataset: MagicMock,
    mock_get_define_xml_contents: MagicMock,
    rule_check_dataset_against_library_and_define: dict,
):
    """
    Test for validating dataset contents against define and library metadata.
    Example rule:
    Library Variable Core Status = Permissible AND
    Define.xml Variable Origin Type = Collected AND
    Variable value is null

    What the test does is:
    1. Saves library metadata to in-memory cache and uses it.
       (The engine pulls variable metadata from cache).
    2. Mocks Define XML download to return test file contents.
    3. Mocks dataset download to return a DataFrame with invalid columns.
    4. Asserts that the errors have been reported properly.
    """
    # use in-memory cache
    os.environ.pop("CACHE_TYPE", None)

    # save library metadata to cache
    variables_metadata: dict = {
        "AE": {
            "AELNKID": {
                "core": "Exp",
            },
            "AESEV": {
                "core": "Perm",
            },
            "AESER": {
                "core": "Perm",
            },
        }
    }
    cache = InMemoryCacheService()
    standard: str = "sdtmig"
    standard_version: str = "3-1-2"
    library_metadata = LibraryMetadataContainer(variables_metadata=variables_metadata)

    # mock define xml download to return test file contents
    test_define_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_defineV21-SDTM.xml"
    )
    with open(test_define_path, "rb") as file:
        contents: bytes = file.read()
    mock_get_define_xml_contents.return_value = contents

    # mock dataset download to return DataFrame with empty values
    mock_get_dataset.return_value = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "AELNKID": ["test", None, "alex"],
                "AESEV": [None, None, "test"],
                "AESER": ["1", "2", None],
            }
        )
    )
    mock_get_dataset_class.return_value = "EVENTS"

    # run the validation and check result
    rules_engine = RulesEngine(
        cache=cache,
        standard=standard,
        standard_version=standard_version,
        library_metadata=library_metadata,
    )
    dataset_metadata = SDTMDatasetMetadata(
        name="AE",
        first_record={"DOMAIN": "AE"},
        filename="filename",
        full_path="study_id/data_bundle_id/filename",
    )
    validation_result: List[dict] = rules_engine.validate_single_dataset(
        rule=rule_check_dataset_against_library_and_define,
        datasets=[dataset_metadata],
        dataset_metadata=dataset_metadata,
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "dataset": "filename",
            "domain": "AE",
            "variables": [
                "AESER",
                "AESEV",
            ],  # AELNKID must not be included since its core status is not "Perm"
            "message": RuleProcessor.extract_message_from_rule(
                rule_check_dataset_against_library_and_define
            ),
            "errors": [
                {
                    "dataset": "filename",
                    "row": 1,
                    "value": {"AESEV": "null", "AESER": "1"},
                },
                {
                    "dataset": "filename",
                    "row": 2,
                    "value": {"AESEV": "null", "AESER": "2"},
                },
                {
                    "dataset": "filename",
                    "row": 3,
                    "value": {"AESEV": "test", "AESER": "null"},
                },
            ],
        }
    ]


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_class")
def test_validate_single_dataset_operation_dataset_larger_than_target_dataset(
    mock_get_dataset_class: MagicMock,
    mock_get_dataset: MagicMock,
    rule_distinct_operation_is_not_contained_by: dict,
):
    """
    Unit test for the rules engine that ensures that
    if the operation result is longer than the target dataset
    the validation is being performed correctly.
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
                "IESEQ": [
                    1,
                ],
                "IETEST": [
                    "Matching value",
                ],
            }
        )
    )
    operation_result_dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "STUDYID": [
                    "CDISCPILOT01",
                    "CDISCPILOT01",
                ],
                "DOMAIN": [
                    "TI",
                    "TI",
                ],
                "IETEST": [
                    "Not a match",
                    "Matching value",
                ],
            }
        )
    )

    path_to_dataset_map: dict = {
        os.path.join("study_id", "data_bundle_id", "ie.xpt"): target_dataset,
        os.path.join("study_id", "data_bundle_id", "ti.xpt"): operation_result_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]
    mock_get_dataset_class.return_value = None
    datasets = [
        SDTMDatasetMetadata(
            name="IE",
            first_record={"DOMAIN": "IE"},
            filename="ie.xpt",
            full_path=os.path.join("study_id", "data_bundle_id", "ie.xpt"),
        ),
        SDTMDatasetMetadata(
            name="TI",
            first_record={"DOMAIN": "TI"},
            filename="ti.xpt",
            full_path=os.path.join("study_id", "data_bundle_id", "ti.xpt"),
        ),
    ]
    validation_result: List[dict] = RulesEngine(
        standard="sdtmig", standard_version="3-4"
    ).validate_single_dataset(
        rule=rule_distinct_operation_is_not_contained_by,
        datasets=datasets,
        dataset_metadata=datasets[0],
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "dataset": "ie.xpt",
            "domain": "IE",
            "variables": [],
            "message": None,
            "errors": [],
        }
    ]


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_metadata"
)
def test_validate_extract_metadata_operation(
    mock_get_dataset_metadata: MagicMock,
    mock_get_dataset: MagicMock,
    rule_equal_to_with_extract_metadata_operation: dict,
):
    """
    Unit test for validating extract_metadata operation.
    The rule applies to SUPPEC domain name and checks that
    value of RDOMAIN equals characters 5 and 6 of the dataset name.
    """
    # mock download of dataset metadata
    mock_get_dataset_metadata.return_value = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "dataset_name": [
                    "SUPPEC",
                ],
            }
        )
    )

    # create a dataset
    dataset = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "RDOMAIN": [
                    "EC",
                    "EC",
                    "EC",
                ],
                "IDVAR": [
                    "ECSEQ",
                    "ECSEQ",
                    "ECSEQ",
                ],
                "IDVARVAL": [
                    1,
                    2,
                    3,
                ],
            }
        )
    )
    mock_get_dataset.return_value = dataset
    dataset_metadata = SDTMDatasetMetadata(
        name="SUPPEC",
        first_record={"RDOMAIN": "EC"},
        filename="suppec.xpt",
        full_path="study_id/data_bundle_id/suppec.xpt",
    )

    # run validation
    validation_result: List[dict] = RulesEngine(
        standard="sdtmig"
    ).validate_single_dataset(
        rule=rule_equal_to_with_extract_metadata_operation,
        datasets=[dataset_metadata],
        dataset_metadata=dataset_metadata,
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "dataset": "suppec.xpt",
            "domain": "SUPPEC",
            "variables": [
                "RDOMAIN",
            ],
            "message": RuleProcessor.extract_message_from_rule(
                rule_equal_to_with_extract_metadata_operation
            ),
            "errors": [
                {
                    "dataset": "suppec.xpt",
                    "row": 1,
                    "value": {
                        "RDOMAIN": "EC",
                    },
                },
                {
                    "dataset": "suppec.xpt",
                    "row": 2,
                    "value": {
                        "RDOMAIN": "EC",
                    },
                },
                {
                    "dataset": "suppec.xpt",
                    "row": 3,
                    "value": {
                        "RDOMAIN": "EC",
                    },
                },
            ],
        }
    ]


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
def test_dataset_references_invalid_whodrug_terms(
    mock_get_dataset: MagicMock,
    rule_dataset_references_invalid_whodrug_terms: dict,
    installed_whodrug_dictionaries: dict,
):
    """
    Unit test for validate_single_dataset function.
    Checks the case when a dataset references invalid whodrug term.
    """
    # create a dataset where 2 rows reference invalid terms
    invalid_df = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "DOMAIN": [
                    "AE",
                    "AE",
                    "AE",
                    "AE",
                ],
                "AETERM": ["A", "B", "B", "B"],
                "AEINA": ["A", "A01", "A01AC", "A01AD"],
            }
        )
    )
    mock_get_dataset.return_value = invalid_df
    cache_service = installed_whodrug_dictionaries["cache_service"]
    cache_service.add(
        "standards/sdtmig/3-4",
        {"classes": [{"name": "EVENTS", "datasets": [{"name": "AE"}]}]},
    )
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "AE"}, filename="dataset_path", full_path="dataset_path"
    )

    # run validation
    engine = RulesEngine(
        cache_service,
        installed_whodrug_dictionaries["data_service"],
        external_dictionaries=ExternalDictionariesContainer(
            {
                DictionaryTypes.WHODRUG.value: installed_whodrug_dictionaries[
                    "whodrug_path"
                ]
            }
        ),
        standard="sdtmig",
        standard_version="3-4",
    )
    validation_result: List[dict] = engine.validate_single_dataset(
        rule=rule_dataset_references_invalid_whodrug_terms,
        datasets=[dataset_metadata],
        dataset_metadata=dataset_metadata,
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "domain": "AE",
            "dataset": "dataset_path",
            "variables": [
                "AEINA",
            ],
            "message": RuleProcessor.extract_message_from_rule(
                rule_dataset_references_invalid_whodrug_terms
            ),
            "errors": [
                {
                    "dataset": "dataset_path",
                    "row": 3,
                    "value": {
                        "AEINA": "A01AC",
                    },
                },
                {
                    "dataset": "dataset_path",
                    "row": 4,
                    "value": {
                        "AEINA": "A01AD",
                    },
                },
            ],
        }
    ]


@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset")
@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_variables_metadata"
)
@patch("cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_class")
def test_validate_variables_order_against_library_metadata(
    mock_get_dataset_class: MagicMock,
    mock_get_variables_metadata: MagicMock,
    mock_get_dataset: MagicMock,
    rule_validate_columns_order_against_library_metadata: dict,
):
    """
    The test validates order of dataset columns against the library metadata.
    """
    # mock dataset download
    dataset_df = PandasDataset(
        pd.DataFrame.from_dict(
            {
                "DOMAIN": [
                    "AE",
                    "AE",
                ],
                "AESEQ": [
                    1,
                    2,
                ],
                "STUDYID": [
                    "TEST_STUDY",
                    "TEST_STUDY",
                ],
                "AETERM": [
                    "test",
                    "test",
                ],
            }
        )
    )
    mock_get_dataset.return_value = dataset_df

    standard: str = "sdtmig"
    standard_version: str = "3-1-2"

    mock_get_variables_metadata.return_value = pd.DataFrame.from_dict(
        {"data": {"variable_name": dataset_df.columns.tolist()}}
    )

    mock_get_dataset_class.return_value = "EVENTS"
    # fill cache
    cache = InMemoryCacheService.get_instance()
    cache_data: dict = {
        "classes": [
            {
                "name": "Events",
                "classVariables": [
                    {"name": "--TERM", "ordinal": 1},
                    {"name": "--SEQ", "ordinal": 2},
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
        ]
    }
    standard_data = {
        "_links": {"model": {"href": "/mdr/sdtm/1-5"}},
        "domains": {
            "HO",
            "CO",
            "SU",
            "PP",
            "TM",
            "TD",
            "SS",
            "TR",
            "CV",
            "EX",
            "RELSPEC",
            "FA",
            "SR",
            "SV",
            "TI",
            "CM",
            "RE",
            "TU",
            "ML",
            "RELSUB",
            "SUPPQUAL",
            "TA",
            "UR",
            "RS",
            "VS",
            "EC",
            "IS",
            "DV",
            "RELREC",
            "PR",
            "SM",
            "EG",
            "MK",
            "TS",
            "DS",
            "PE",
            "DM",
            "MH",
            "GF",
            "BE",
            "OE",
            "CE",
            "CP",
            "MS",
            "DD",
            "TV",
            "MI",
            "FT",
            "PC",
            "RP",
            "IE",
            "TE",
            "LB",
            "BS",
            "QS",
            "SC",
            "AG",
            "DA",
            "SE",
            "AE",
            "OI",
            "MB",
            "NV",
        },
        "classes": [
            {
                "name": "Events",
                "datasets": [
                    {
                        "name": "AE",
                        "datasetVariables": [
                            {"name": "AETERM", "ordinal": 3},
                            {"name": "AESEQ", "ordinal": 4},
                        ],
                    }
                ],
            }
        ],
    }
    library_metadata = LibraryMetadataContainer(
        model_metadata=cache_data, standard_metadata=standard_data
    )
    dataset_metadata = SDTMDatasetMetadata(
        first_record={"DOMAIN": "AE"},
        filename="dataset_path",
        full_path="dataset_path",
    )
    # run validation
    engine = RulesEngine(
        cache=cache,
        standard=standard,
        standard_version=standard_version,
        library_metadata=library_metadata,
    )

    def mock_cached_method(*args, **kwargs):
        return mock_get_dataset.return_value

    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_raw_dataset_metadata",
        side_effect=mock_cached_method,
    ):
        result: List[dict] = engine.validate_single_dataset(
            rule_validate_columns_order_against_library_metadata,
            [dataset_metadata],
            dataset_metadata,
        )
    assert result == [
        {
            "executionStatus": "success",
            "dataset": "dataset_path",
            "domain": "AE",
            "variables": [
                "$column_order_from_dataset",
                "$column_order_from_library",
                "AESEQ",
                "AETERM",
                "DOMAIN",
                "STUDYID",
            ],
            "message": RuleProcessor.extract_message_from_rule(
                rule_validate_columns_order_against_library_metadata
            ),
            "errors": [
                {
                    "value": {
                        "$column_order_from_library": [
                            "STUDYID",
                            "DOMAIN",
                            "AETERM",
                            "AESEQ",
                            "TIMING_VAR",
                        ],
                        "$column_order_from_dataset": [
                            "DOMAIN",
                            "AESEQ",
                            "STUDYID",
                            "AETERM",
                        ],
                        "AESEQ": 1,
                        "AETERM": "test",
                        "DOMAIN": "AE",
                        "STUDYID": "TEST_STUDY",
                    },
                    "dataset": "dataset_path",
                }
            ],
        }
    ]
