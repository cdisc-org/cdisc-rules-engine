import os
import pytest
import pandas as pd
from typing import List
from unittest.mock import patch, MagicMock

from engine.exceptions.custom_exceptions import VariableMetadataNotFoundError
from engine.models.rule_conditions import ConditionCompositeFactory
from engine.rules_engine import RulesEngine
from engine.services.cache.in_memory_cache_service import InMemoryCacheService
from engine.utilities.utils import get_library_variables_metadata_cache_key, get_standard_details_cache_key
from engine.utilities.rule_processor import RuleProcessor
from engine.enums.execution_status import ExecutionStatus

from conftest import get_matches_regex_pattern_rule


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
    generate_dataset_errors_metadata = {
        "name": "generate_dataset_errors",
        "label": "Generate Dataset Errors",
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
    assert generate_dataset_errors_metadata in schema["actions"]
    assert generate_single_error_metadata in schema["actions"]


def test_validate_rule_invalid_suffix(
    mock_ae_record_rule_equal_to_suffix: dict,
):
    """
    Unit test for function validate_rule.
    Test the case when we are checking a string suffix.
    Dataset has 2 strings: valid and invalid.
    """
    dataset_mock = pd.DataFrame.from_dict(
        {
            "AESTDY": [
                "valid-test",
                "test-invalid",
            ],
        }
    )
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            mock_ae_record_rule_equal_to_suffix, "study/bundle", [{}], "AE"
        )
        assert validation_result == [
            "1 row(s) with error: Suffix of AESTDY is equal to test.",
        ]


def test_validate_rule_invalid_prefix(
    mock_record_rule_equal_to_string_prefix: dict,
):
    """
    Unit test for function validate_rule.
    Test the case when we are checking a string prefix.
    Dataset has 2 strings: valid and invalid.
    """
    dataset_mock = pd.DataFrame.from_dict(
        {
            "AESTDY": [
                "test-valid",
                "invalid-test",
            ],
        }
    )
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            mock_record_rule_equal_to_string_prefix, "study/bundle", [{}], "AE"
        )
        assert validation_result == [
            "1 row(s) with error: Prefix of AESTDY is equal to test.",
        ]


def test_validate_rule_cross_dataset_check(dataset_rule_equal_to: dict):
    """
    The test checks that a rule can be executed for several datasets.
    We have 2 datasets that have common STUDYID and SUBJECTID columns and
    need to validate only that records from 1st dataset
    whose STUDYID and SUBJECTID present in the 2nd dataset.
    """
    # create datasets
    ec_dataset = pd.DataFrame.from_dict(
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
    # mock blob storage call
    path_to_dataset_map: dict = {
        "path/ae.xpt": ae_dataset,
        "path/ec.xpt": ec_dataset,
    }
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        datasets: List[dict] = [
            {"domain": "EC", "filename": "ec.xpt"},
            {"domain": "AE", "filename": "ae.xpt"},
        ]
        validation_result: List[str] = RulesEngine().validate_single_rule(
            dataset_rule_equal_to, "path/ec.xpt", datasets, "EC"
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "EC",
                "variables": ["ECSTDY"],
                "message": "Value of ECSTDY is equal to AESTDY.",
                "errors": [
                    {
                        "row": 1,
                        "value": {"ECSTDY": 4.0},
                        "uSubjId": "CDISC001",
                        "seq": 1,
                    },
                    {
                        "row": 2,
                        "value": {"ECSTDY": 5.0},
                        "uSubjId": "CDISC001",
                        "seq": 2,
                    },
                ],
            }
        ]


def test_validate_one_to_one_rel_across_datasets(dataset_rule_one_to_one_related: dict):
    """
    The test checks validation of one-to-one relationship
    across two datasets.
    """
    datasets: List[dict] = [
        {"domain": "EC", "filename": "ec.xpt"},
        {"domain": "AE", "filename": "ae.xpt"},
    ]
    ae_dataset = pd.DataFrame.from_dict(
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
    # this dataset violates one-to-one relationship and should flag an error
    ec_dataset = pd.DataFrame.from_dict(
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
    path_to_dataset_map: dict = {
        "path/ae.xpt": ae_dataset,
        "path/ec.xpt": ec_dataset,
    }
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            dataset_rule_one_to_one_related, "path/ec.xpt", datasets, "EC"
        )
        assert validation_result == [
            "3 row(s) with error: VISITNUM is not one-to-one related to VISIT"
        ]


def test_validate_rule_single_dataset_check(dataset_rule_greater_than: dict):
    """
    The test checks the rules validation for a single dataset.
    In this case the rules does not have "datasets" key
    and datasets map is also empty.
    """
    dataset_mock = pd.DataFrame.from_dict(
        {
            "ECCOOLVAR": [20, 100, 10, 34],
            "AESTDY": [1, 2, 40, 50],
        }
    )
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            dataset_rule_greater_than, "study/bundle", [{}], "EC"
        )
        assert len(validation_result) == 1
        assert validation_result == [
            "2 row(s) with error: Value for ECCOOLVAR greater than 30."
        ]


def test_validate_rule_equal_length(dataset_rule_has_equal_length: dict):
    """
    The test checks validation of column length.
    The case when rule needs to find records whose length is
    equal to a desired value.
    For example, check all ECCOOLVAR columns whose length is equal to 5.
    """
    dataset_mock = pd.DataFrame.from_dict(
        {
            "ECCOOLVAR": ["first_string", "equal"],
            "AESTDY": ["pokemon", "test"],
        }
    )
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            dataset_rule_has_equal_length, "study/bundle", [{}], "EC"
        )
        assert len(validation_result) == 1
        assert validation_result == [
            "1 row(s) with error: Length of ECCOOLVAR is equal to 5."
        ]


def test_validate_is_contained_by_distinct(mock_rule_distinct_operation: dict):
    datasets: List[dict] = [
        {"domain": "DM", "filename": "dm.xpt"},
        {"domain": "AE", "filename": "ae.xpt"},
    ]
    ae_dataset = pd.DataFrame.from_dict({"AESTDY": [1, 2, 3, 5000]})

    dm_dataset = pd.DataFrame.from_dict({"USUBJID": [1, 2, 2, 3, 4, 5, 5, 3, 3, 3]})

    path_to_dataset_map: dict = {
        "path/ae.xpt": ae_dataset,
        "path/dm.xpt": dm_dataset,
    }
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            mock_rule_distinct_operation, "path/ae.xpt", datasets, "AE"
        )
        assert len(validation_result) == 1
        result = validation_result[0]
        assert "errors" in result
        assert len(result.get("errors")) == 1
        error = result.get("errors")[0]
        assert error["row"] == 4


def test_validate_rule_not_equal_length(dataset_rule_has_not_equal_length: dict):
    """
    The test checks validation of column length.
    The case when rule needs to find records whose length is
    not equal to a desired value.
    For example, check all ECCOOLVAR columns whose length is not equal to 5.
    """
    dataset_mock = pd.DataFrame.from_dict(
        {
            "ECCOOLVAR": ["first_string", "valid"],
            "AESTDY": ["pokemon", "test"],
        }
    )
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            dataset_rule_has_not_equal_length, "study/bundle", [{}], "EC"
        )
        assert len(validation_result) == 1
        assert validation_result == [
            "1 row(s) with error: Length of ECCOOLVAR is not equal to 5."
        ]


def test_validate_rule_multiple_conditions(dataset_rule_multiple_conditions: dict):
    dataset_mock = pd.DataFrame.from_dict(
        {
            "ECCOOLVAR": ["first_string", "valid", "cool"],
            "AESTDY": ["pokemon", "test", "item"],
        }
    )
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            dataset_rule_multiple_conditions, "study/bundle", [{}], "EC"
        )
        assert len(validation_result) == 1
        assert validation_result == [
            "2 row(s) with error: Length of ECCOOLVAR is not equal to 5 or ECCOOLVAR == cool."
        ]


def test_validate_record_rule_numbers_separated_by_dash_pattern():
    """
    The test checks matching "{number}-{number}" pattern.
    """
    number_number_pattern: str = "^\d+\-\d+$"
    rule: dict = get_matches_regex_pattern_rule(number_number_pattern)
    dataset_mock = pd.DataFrame.from_dict({"AESTDY": ["5-5", "10-10", "test"]})
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            rule, "study/bundle", [{}], "AE"
        )
        assert validation_result == [
            f"2 row(s) with error: Records have the following pattern: {number_number_pattern}"
        ]


def test_validate_record_rule_semi_colon_delimited_pattern():
    """
    The test checks matching semi-colon delimited pattern.
    """
    semi_colon_delimited_pattern: str = "[^,]*;[^,]*"
    rule: dict = get_matches_regex_pattern_rule(semi_colon_delimited_pattern)
    dataset_mock = pd.DataFrame.from_dict({"AESTDY": ["5;5", "alex;alex", "test"]})
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            rule, "study/bundle", [{}], "AE"
        )
        assert validation_result == [
            f"2 row(s) with error: Records have the following pattern: {semi_colon_delimited_pattern}"
        ]


def test_validate_record_rule_no_letters_numbers_underscores():
    """
    The test checks that we can match a pattern like:
    A string contains characters other than letters, numbers or underscores.
    """
    # checks that string contains characters other than letters, numbers or underscores
    does_not_contain_pattern: str = "^((?![a-zA-Z0-9_]).)*$"
    rule: dict = get_matches_regex_pattern_rule(does_not_contain_pattern)
    dataset_mock = pd.DataFrame.from_dict({"AESTDY": ["[.*)]#@", "alex", "|>.§!"]})
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            rule, "study/bundle", [{}], "AE"
        )
        assert validation_result == [
            f"2 row(s) with error: Records have the following pattern: {does_not_contain_pattern}"
        ]


def test_validate_dataset_metadata(dataset_metadata_not_equal_to_rule: dict):
    """
    Unit test that checks dataset metadata validation.
    """
    dataset_mock = pd.DataFrame.from_dict(
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
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset_metadata",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            dataset_metadata_not_equal_to_rule, "study/bundle", [{}], "EC"
        )
        assert validation_result == [
            {
                "domain": "EC",
                "errors": [],
                "executionStatus": "success",
                "message": None,
                "variables": [],
            }
        ]


def test_validate_dataset_metadata_wrong_metadata(
    dataset_metadata_not_equal_to_rule: dict,
):
    """
    Unit test that checks dataset metadata validation.
    Test the case when dataset contains the wrong data.
    """
    dataset_mock = pd.DataFrame.from_dict(
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
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset_metadata",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            dataset_metadata_not_equal_to_rule, "study/bundle", [{}], "EC"
        )
        assert validation_result == [
            {
                "domain": "EC",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "variables": ["dataset_label", "dataset_name", "dataset_size"],
                "errors": [
                    {
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


def test_validate_variable_metadata(variables_metadata_rule: dict):
    """
    Unit test that checks variable metadata validation.
    """
    dataset_mock = pd.DataFrame.from_dict(
        {
            "variable_name": ["STUDYID", "DOMAIN"],
            "variable_size": [5, 20],
            "variable_label": ["Study Identifier", "Domain Name"],
            "variable_data_type": ["Char", "Char"],
        }
    )
    with patch(
        "engine.services.local_data_service.LocalDataService.get_variables_metadata",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            variables_metadata_rule, "study/bundle", [{}], "EC"
        )
        assert validation_result == [
            {
                "domain": "EC",
                "errors": [],
                "executionStatus": "success",
                "message": None,
                "variables": [],
            }
        ]


def test_validate_variable_metadata_wrong_metadata(variables_metadata_rule: dict):
    """
    Unit test that checks variable metadata validation.
    Test the case when variable metadata is wrong.
    """
    dataset_mock = pd.DataFrame.from_dict(
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
    with patch(
        "engine.services.local_data_service.LocalDataService.get_variables_metadata",
        return_value=dataset_mock,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            variables_metadata_rule, "study/bundle", [{}], "EC"
        )
        assert validation_result == [
            {
                "domain": "EC",
                "variables": ["variable_data_type", "variable_label", "variable_name"],
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "errors": [
                    {
                        "row": 1,
                        "value": {
                            "variable_name": "longer than eight",
                            "variable_label": "Study Identifier Very Long Longer than 40",
                            "variable_data_type": "Char",
                        },
                    },
                    {
                        "row": 2,
                        "value": {
                            "variable_name": "longer than eight as well",
                            "variable_label": "Long Long Label Very Long Longer than 40 chars",
                            "variable_data_type": "Char",
                        },
                    },
                ],
                "message": "Variable metadata is wrong.",
            }
        ]


def test_rule_with_domain_prefix_replacement():
    rule = {
        "core_id": "TEST1",
        "standards": [],
        "domains": {"Include": ["All"]},
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
    df = pd.DataFrame.from_dict({"AESTDY": [11, 12, 40, 59, 59]})
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=df,
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            rule, "study/bundle", [{"domain": "AE", "filename": "ae.xpt"}], "AE"
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "AE",
                "variables": ["AESTDY"],
                "message": "Invalid AESTDY value",
                "errors": [
                    {"row": 1, "value": {"AESTDY": 11}},
                    {"row": 2, "value": {"AESTDY": 12}},
                    {"row": 3, "value": {"AESTDY": 40}},
                    {"row": 4, "value": {"AESTDY": 59}},
                    {"row": 5, "value": {"AESTDY": 59}},
                ],
            }
        ]


@pytest.mark.parametrize(
    "datasets, expected_validation_result",
    [
        (
            [
                {"domain": "AE", "filename": "ae.xpt"},
                {"domain": "EC", "filename": "ec.xpt"},
            ],
            ["1 row(s) with error: Domain AE exists"],
        ),
        (
            {},
            [
                {
                    "domain": "AE",
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
    domain_presence_rule: dict, datasets: List[dict], expected_validation_result: list
):
    """
    Unit test for RulesEngine.validate_domain_presence.
    """
    actual_validation_result = RulesEngine().validate_single_rule(
        domain_presence_rule,
        "study/bundle",
        datasets,
        "AE",
    )
    assert actual_validation_result == expected_validation_result


def test_validate_single_rule(dataset_rule_equal_to_error_objects: dict):
    """
    Unit test for validate_single_rule function.
    """
    df = pd.DataFrame.from_dict(
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
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=df,
    ):
        dataset_domain: str = "AE"
        datasets: List[dict] = [{"domain": dataset_domain, "filename": "ae.xpt"}]
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            dataset_rule_equal_to_error_objects,
            "study/bundle",
            datasets,
            dataset_domain,
        )
        assert validation_result == [
            {
                "domain": "AE",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "variables": ["AESTDY"],
                "errors": [
                    {
                        "row": 1,
                        "value": {
                            "AESTDY": "test",
                        },
                        "uSubjId": "1",
                        "seq": 1,
                    },
                    {
                        "row": 4,
                        "value": {
                            "AESTDY": "test",
                        },
                        "uSubjId": "1",
                        "seq": 4,
                    },
                    {
                        "row": 5,
                        "value": {
                            "AESTDY": "test",
                        },
                        "uSubjId": "3",
                        "seq": 5,
                    },
                ],
                "message": "Value of AESTDY is equal to test.",
            }
        ]


def test_validate_single_rule_not_equal_to(
    dataset_rule_not_equal_to_error_objects: dict,
):
    """
    Unit test for validate_single_rule function.
    Checks the case when all rule conditions are wrapped
    into "not" block.
    """
    df = pd.DataFrame.from_dict(
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
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=df,
    ):
        dataset_domain: str = "AE"
        datasets: List[dict] = [{"domain": dataset_domain, "filename": "ae.xpt"}]
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            dataset_rule_not_equal_to_error_objects,
            "study/data_bundle",
            datasets,
            dataset_domain,
        )
        assert validation_result == [
            {
                "domain": "AE",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "variables": ["AESTDY"],
                "errors": [
                    {
                        "row": 2,
                        "value": {
                            "AESTDY": "alex",
                        },
                        "uSubjId": "2",
                        "seq": 2,
                    },
                    {
                        "row": 3,
                        "value": {
                            "AESTDY": "alex",
                        },
                        "uSubjId": "2",
                        "seq": 3,
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
            {
                "dataset_name": "AE",
                "dataset_label": "Adverse Events",
                "dataset_location": "ae.xpt",
            },
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
            ),
            [
                {
                    "domain": "AE",
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "variables": ["dataset_label", "dataset_location", "dataset_name"],
                    "errors": [
                        {
                            "row": 1,
                            "value": {
                                "dataset_name": "AE",
                                "dataset_label": "Adverse Events",
                                "dataset_location": "te.xpt",
                            },
                        },
                    ],
                    "message": "Dataset metadata does not correspond to Define XML",
                }
            ],
        ),
        (
            {
                "dataset_name": "AE",
                "dataset_label": "Adverse Events",
                "dataset_location": "ae.xpt",
            },
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
            ),
            [
                {
                    "domain": "AE",
                    "errors": [],
                    "executionStatus": "success",
                    "message": None,
                    "variables": [],
                }
            ],
        ),
    ],
)
@patch("engine.rules_engine.RulesEngine.get_define_xml_metadata_for_domain")
def test_validate_dataset_metadata_against_define_xml(
    mock_get_define_xml_metadata_for_domain,
    define_xml_validation_rule: dict,
    define_xml_metadata: dict,
    dataset_mock: pd.DataFrame,
    expected_validation_result: List[dict],
):
    """
    Unit test for Define XML validation.
    Creates an invalid dataset and validates it against Define XML.
    """
    mock_get_define_xml_metadata_for_domain.return_value = define_xml_metadata
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset_metadata",
        return_value=dataset_mock,
    ):
        datasets: List[dict] = [{"domain": "AE", "filename": "ae.xpt"}]
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            define_xml_validation_rule, "CDISC01/test/ae.xpt", datasets, "AE"
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
            ),
            [
                {
                    "domain": "AE",
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "variables": ["variable_size"],
                    "errors": [{"row": 1, "value": {"variable_size": 30}}],
                    "message": "Variable metadata variable_size does not match define variable size",
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
            ),
            [
                {
                    "domain": "AE",
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "variables": ["variable_size"],
                    "errors": [{"row": 1, "value": {"variable_size": 30}}],
                    "message": "Variable metadata variable_size does not match define variable size",
                }
            ],
        ),
    ],
)
@patch("engine.rules_engine.RulesEngine.get_define_xml_variables_metadata")
def test_validate_variable_metadata_against_define_xml(
    mock_get_define_xml_variables_metadata: MagicMock,
    define_xml_variable_validation_rule: dict,
    variable_metadata: dict,
    dataset_mock: pd.DataFrame,
    expected_validation_result: List[dict],
):
    """
    Unit test for Define XML validation.
    Creates an invalid dataset and validates it against Define XML.
    """
    mock_get_define_xml_variables_metadata.return_value = variable_metadata
    with patch(
        "engine.services.local_data_service.LocalDataService.get_variables_metadata",
        return_value=dataset_mock,
    ):
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            dataset_domain="AE",
            dataset_path="CDISC01/test",
            rule=define_xml_variable_validation_rule,
            datasets=[{"domain": "AE", "filename": "ae.xpt"}],
        )
        assert validation_result == expected_validation_result


@patch("engine.rules_engine.RulesEngine.get_define_xml_value_level_metadata")
def test_validate_value_level_metadata_against_define_xml(
    mock_get_define_xml_value_level_metadata,
    define_xml_value_level_metadata_validation_rule: dict,
):
    def check_length_func(row):
        return len(row["AETERM"]) < 10

    def filter_func(row):
        return row["FILTER"] == "SHORT"

    df = pd.DataFrame.from_dict(
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
    mock_get_define_xml_value_level_metadata.return_value = [
        {
            "define_variable_name": "AETERM",
            "filter": filter_func,
            "length_check": check_length_func,
        }
    ]
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=df,
    ):
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            dataset_domain="AE",
            dataset_path="CDISC01/test",
            rule=define_xml_value_level_metadata_validation_rule,
            datasets=[{"domain": "AE", "filename": "CDISC01/test/ae.xpt"}],
        )
        assert validation_result == [
            {
                "domain": "AE",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "variables": [
                    "AETERM",
                ],
                "errors": [
                    {
                        "row": 2,
                        "value": {"AETERM": "A" * 200},
                        "uSubjId": "5",
                        "seq": 2,
                    },
                    {
                        "row": 4,
                        "value": {"AETERM": "A" * 15},
                        "uSubjId": "5",
                        "seq": 4,
                    },
                ],
                "message": "Variable data does not match length specified by value level metadata in define.xml",
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
                    "executionStatus": ExecutionStatus.SKIPPED.value,
                    "variables": [],
                    "message": None,
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
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "variables": ["AESTDY"],
                    "errors": [
                        {"row": 1, "value": {"AESTDY": "test"}, "uSubjId": "1"},
                        {"row": 4, "value": {"AESTDY": "test"}, "uSubjId": "1"},
                        {"row": 8, "value": {"AESTDY": "test"}, "uSubjId": "2"},
                    ],
                    "message": "Value of AESTDY is equal to test.",
                }
            ],
        ),
    ],
)
def test_validate_split_dataset_contents(
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
    first_dataset_part: pd.DataFrame = pd.DataFrame.from_dict(
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
    second_dataset_part: pd.DataFrame = pd.DataFrame.from_dict(
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
    # mock blob storage call and execute the validation
    with patch(
        "engine.services.local_data_service.LocalDataService._async_get_datasets",
        return_value=[first_dataset_part, second_dataset_part],
    ):
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            dataset_domain="AE",
            dataset_path="CDISC01/test/ae.xpt",
            rule=dataset_rule_equal_to_error_objects,
            datasets=[
                {"domain": "AE", "filename": "ae.xpt"},
                {"domain": "AE", "filename": "ae_1.xpt"},
            ],
        )
        # check validation result
        assert validation_result == result


def test_validate_split_dataset_metadata(dataset_metadata_not_equal_to_rule: dict):
    """
    Unit test for validating metadata of a split dataset.
    """
    # create two dataframes
    first_dataset_part: pd.DataFrame = pd.DataFrame.from_dict(
        {
            "dataset_size": [
                5,
            ],
            "dataset_location": [
                "ec.xpt",
            ],
            "dataset_name": [
                "EC",
            ],
            "dataset_label": [
                "EC Label",
            ],
        }
    )
    second_dataset_part: pd.DataFrame = pd.DataFrame.from_dict(
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
    # mock blob storage call and execute the validation
    with patch(
        "engine.services.local_data_service.LocalDataService._async_get_datasets",
        return_value=[first_dataset_part, second_dataset_part],
    ):
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            dataset_domain="EC",
            dataset_path="CDISC01/test/ec.xpt",
            rule=dataset_metadata_not_equal_to_rule,
            datasets=[
                {"domain": "EC", "filename": "ec.xpt"},
                {"domain": "EC", "filename": "ec_1.xpt"},
            ],
        )
        # check validation result. error is contained only in the second part of the dataset.
        assert validation_result == [
            {
                "domain": "EC",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "errors": [
                    {
                        "row": 2,
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


def test_validate_split_dataset_variables_metadata(variables_metadata_rule: dict):
    """
    Unit test for validating variables metadata of a split dataset.
    """
    first_dataset_part = pd.DataFrame.from_dict(  # this part should flag an error
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
    second_dataset_part = pd.DataFrame.from_dict(
        {
            "variable_name": ["STUDYID", "DOMAIN"],
            "variable_size": [5, 20],
            "variable_label": ["Study Identifier", "Domain Name"],
            "variable_data_type": ["Char", "Char"],
        }
    )
    with patch(
        "engine.services.local_data_service.LocalDataService._async_get_datasets",
        return_value=[
            first_dataset_part,
            second_dataset_part,
        ],
    ):
        validation_result: List[str] = RulesEngine().validate_single_rule(
            rule=variables_metadata_rule,
            dataset_path="CDISC/test/ec.xpt",
            datasets=[
                {"domain": "EC", "filename": "ec.xpt"},
                {"domain": "EC", "filename": "ec_1.xpt"},
            ],
            dataset_domain="EC",
        )
        assert validation_result == [
            {
                "domain": "EC",
                "executionStatus": ExecutionStatus.SUCCESS.value,
                "variables": ["variable_data_type", "variable_label", "variable_name"],
                "errors": [
                    {
                        "row": 1,
                        "value": {
                            "variable_label": "Study Identifier Study Identifier Very Long",
                            "variable_name": "STUDYIDLONG",
                            "variable_data_type": "Char",
                        },
                    }
                ],
                "message": "Variable metadata is wrong.",
            }
        ]


@pytest.mark.parametrize(
    "variable_metadata, allowed_terms_map, expected_validation_result",
    [
        (
            [
                {
                    "define_variable_name": "TEST",
                    "define_variable_label": "TEST LABEL",
                    "define_variable_size": 20,
                    "define_variable_role": "VAR ROLE",
                    "define_variable_data_type": "Char",
                    "define_variable_allowed_terms": ["DEAD", "deceased"],
                    "define_variable_ccode": "C12345",
                }
            ],
            {"C12345": ["dead", "deceased"]},
            [
                {
                    "domain": "EC",
                    "executionStatus": ExecutionStatus.SUCCESS.value,
                    "variables": [
                        "define_variable_allowed_terms",
                        "define_variable_ccode",
                        "define_variable_name",
                    ],
                    "errors": [
                        {
                            "row": 1,
                            "value": {
                                "define_variable_ccode": "C12345",
                                "define_variable_name": "TEST",
                                "define_variable_allowed_terms": ["DEAD", "deceased"],
                            },
                        }
                    ],
                    "message": "Define specifies invalid codelist terms",
                }
            ],
        )
    ],
)
@patch("engine.rules_engine.RulesEngine.get_define_xml_variables_metadata")
def test_validate_define_ct_allowed_terms(
    mock_get_define_xml_variables_metadata: MagicMock,
    define_xml_allowed_terms_check_rule: dict,
    variable_metadata: dict,
    allowed_terms_map: pd.DataFrame,
    expected_validation_result: List[dict],
):
    cache = InMemoryCacheService()
    ct_package = "sdtmct-2021-12-17"
    standard = "sdtmig"
    standard_version = "3-3"
    cache.add(ct_package, allowed_terms_map)
    mock_get_define_xml_variables_metadata.return_value = variable_metadata
    rules_engine = RulesEngine(
        cache=cache, ct_package=ct_package, standard=standard, standard_version=standard_version
    )
    result = rules_engine.validate_define_xml(
        define_xml_allowed_terms_check_rule,
        "test/",
        [{"domain": "EC", "filename": "ec.xpt"}],
        "EC",
    )
    assert result == expected_validation_result


def test_validate_record_in_parent_domain(
    dataset_rule_record_in_parent_domain_equal_to: dict,
):
    """
    Unit test for validating value of a column in parent domain.
    """
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
    path_to_dataset_map: dict = {
        "path/ec.xpt": ec_dataset,
        "path/suppec.xpt": suppec_dataset,
    }
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        side_effect=lambda dataset_name: path_to_dataset_map[dataset_name],
    ):
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
        validation_result: List[str] = RulesEngine().validate_single_rule(
            dataset_rule_record_in_parent_domain_equal_to, "path/ec.xpt", datasets, "EC"
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "EC",
                "variables": ["ECPRESP", "QNAM"],
                "message": "Dataset contents is wrong.",
                "errors": [
                    {
                        "row": 1,
                        "value": {"ECPRESP": "Y", "QNAM": "ECREASOC"},
                        "uSubjId": "CDISC005",
                        "seq": 4,
                    }
                ],
            }
        ]


def test_validate_additional_columns(dataset_rule_additional_columns_not_null: dict):
    """
    Unit test for validating additional columns like TSVAL1, TSVAL2.
    """
    dataset = pd.DataFrame.from_dict(
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
    with patch(
        "engine.services.local_data_service.LocalDataService.get_dataset",
        return_value=dataset,
    ):
        validation_result: List[dict] = RulesEngine().validate_single_rule(
            rule=dataset_rule_additional_columns_not_null,
            dataset_path="CDISC01/test/ts.xpt",
            datasets=[
                {
                    "domain": "TS",
                    "filename": "ts.xpt",
                },
            ],
            dataset_domain="TS",
        )
        assert validation_result == [
            {
                "executionStatus": "success",
                "domain": "TS",
                "variables": ["TSVAL1", "TSVAL2", "TSVAL3"],
                "message": "Additional columns for TSVAL are empty.",
                "errors": [
                    {
                        "row": 2,
                        "value": {
                            "TSVAL1": None,
                            "TSVAL2": "value 2",
                            "TSVAL3": "value 3",
                        },
                        "uSubjId": "1",
                    },
                    {
                        "row": 4,
                        "value": {
                            "TSVAL1": "value",
                            "TSVAL2": None,
                            "TSVAL3": "value 3",
                        },
                        "uSubjId": "1",
                    },
                ],
            }
        ]


@patch("engine.services.local_data_service.LocalDataService.get_define_xml_contents")
@patch("engine.services.local_data_service.LocalDataService.get_dataset")
def test_validate_dataset_contents_against_define_and_library_variable_metadata(
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
    cache = InMemoryCacheService()
    standard: str = "sdtmig"
    standard_version: str = "3-1-2"
    cache.add(
        get_library_variables_metadata_cache_key(standard, standard_version),
        variables_metadata,
    )

    # mock define xml download to return test file contents
    test_define_path: str = (
        f"{os.path.dirname(__file__)}/../resources/test_defineV21-SDTM.xml"
    )
    with open(test_define_path, "rb") as file:
        contents: bytes = file.read()
    mock_get_define_xml_contents.return_value = contents

    # mock dataset download to return DataFrame with empty values
    mock_get_dataset.return_value = pd.DataFrame.from_dict(
        {
            "AELNKID": ["test", None, "alex"],
            "AESEV": [None, None, "test"],
            "AESER": ["1", "2", None],
        }
    )

    # run the validation and check result
    rules_engine = RulesEngine(
        cache=cache,
        standard=standard,
        standard_version=standard_version,
    )
    validation_result: List[dict] = rules_engine.validate_single_rule(
        rule=rule_check_dataset_against_library_and_define,
        dataset_path="study_id/data_bundle_id/filename",
        datasets=[
            {
                "domain": "AE",
                "filename": "ae.xpt",
            },
        ],
        dataset_domain="AE",
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "domain": "AE",
            "variables": [
                "AESER",
                "AESEV",
            ],  # AELNKID must not be included since its core status is not "Perm"
            "message": RuleProcessor.extract_message_from_rule(
                rule_check_dataset_against_library_and_define
            ),
            "errors": [
                {"row": 1, "value": {"AESEV": None, "AESER": "1"}},
                {"row": 2, "value": {"AESEV": None, "AESER": "2"}},
                {"row": 3, "value": {"AESEV": "test", "AESER": None}},
            ],
        }
    ]


@patch("engine.services.local_data_service.LocalDataService.get_dataset")
def test_validate_dataset_contents_against_library_metadata(
    mock_get_dataset: MagicMock,
    rule_check_dataset_contents_against_library_metadata: dict,
):
    """
    Test for validating dataset contents against library metadata.
    The rule only provides variable names and the engine automatically
    validates their values based on the library metadata.

    What the test does is:
    1. Saves library metadata to in-memory cache and uses it.
       (The engine pulls variable metadata from cache).
    2. Mocks dataset download to return a DataFrame with invalid columns.
    3. Asserts that the errors have been reported properly.
    """
    # use in-memory cache
    os.environ.pop("CACHE_TYPE", None)

    # save library metadata to cache
    standard: str = "sdtmig"
    standard_version: str = "3-1-2"
    variables_metadata: dict = {
        "STUDYID": {
            "core": "Req",
        },
        "DOMAIN": {
            "core": "Req",
        },
        "AEORRES": {
            "core": "Exp",
        },
    }
    cache = InMemoryCacheService()
    cache.add(
        get_library_variables_metadata_cache_key(standard, standard_version),
        variables_metadata,
    )
    # mock dataset download to return DataFrame with empty values
    mock_get_dataset.return_value = pd.DataFrame.from_dict(
        {
            "STUDYID": ["CDISC01", None, "CDISC01"],
            "DOMAIN": [
                "AE",
                "AE",
                None,
            ],
        }
    )

    # run the validation and check result
    rules_engine = RulesEngine(
        cache=cache,
        standard=standard,
        standard_version=standard_version,
    )
    validation_result: List[dict] = rules_engine.validate_single_rule(
        rule=rule_check_dataset_contents_against_library_metadata,
        dataset_path="study_id/data_bundle_id/filename",
        datasets=[
            {
                "domain": "AE",
                "filename": "ae.xpt",
            },
        ],
        dataset_domain="AE",
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "domain": "AE",
            "variables": ["AEORRES", "DOMAIN", "STUDYID"],
            "message": RuleProcessor.extract_message_from_rule(
                rule_check_dataset_contents_against_library_metadata
            ),
            "errors": [
                {
                    "row": 1,
                    "value": {
                        "STUDYID": "CDISC01",
                        "DOMAIN": "AE",
                        "AEORRES": "Not in dataset",
                    },
                },
                {
                    "row": 2,
                    "value": {
                        "STUDYID": None,
                        "DOMAIN": "AE",
                        "AEORRES": "Not in dataset",
                    },
                },
                {
                    "row": 3,
                    "value": {
                        "STUDYID": "CDISC01",
                        "DOMAIN": None,
                        "AEORRES": "Not in dataset",
                    },
                },
            ],
        }
    ]


@patch("engine.services.local_data_service.LocalDataService.get_dataset")
def test_validate_dataset_contents_against_library_metadata_no_required_column(
    mock_get_dataset: MagicMock,
    rule_check_dataset_contents_against_library_metadata: dict,
):
    """
    Test for validating dataset contents against library metadata.
    The rule only provides variable names and the engine automatically
    validates their values based on the library metadata.

    The test checks the case when there is no Required column in the dataset.
    """
    # use in-memory cache
    os.environ.pop("CACHE_TYPE", None)

    # save library metadata to cache
    standard: str = "sdtmig"
    standard_version: str = "3-1-2"
    variables_metadata: dict = {
        "STUDYID": {
            "core": "Req",
        },
        "DOMAIN": {
            "core": "Req",
        },
        "AEORRES": {
            "core": "Exp",
        },
    }
    cache = InMemoryCacheService()
    cache.add(
        get_library_variables_metadata_cache_key(standard, standard_version),
        variables_metadata,
    )
    # mock dataset download to return DataFrame with empty values
    mock_get_dataset.return_value = pd.DataFrame.from_dict(
        {
            "DOMAIN": [
                "AE",
                "AE",
                None,
            ],
        }
    )

    # run the validation and check result
    rules_engine = RulesEngine(
        cache = cache,
        standard=standard,
        standard_version=standard_version,
    )
    validation_result: List[dict] = rules_engine.validate_single_rule(
        rule=rule_check_dataset_contents_against_library_metadata,
        dataset_path="study_id/data_bundle_id/filename",
        datasets=[
            {
                "domain": "AE",
                "filename": "ae.xpt",
            },
        ],
        dataset_domain="AE",
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "domain": "AE",
            "variables": ["AEORRES", "DOMAIN", "STUDYID"],
            "message": RuleProcessor.extract_message_from_rule(
                rule_check_dataset_contents_against_library_metadata
            ),
            "errors": [
                {
                    "row": 1,
                    "value": {
                        "STUDYID": "Not in dataset",
                        "DOMAIN": "AE",
                        "AEORRES": "Not in dataset",
                    },
                },
                {
                    "row": 2,
                    "value": {
                        "STUDYID": "Not in dataset",
                        "DOMAIN": "AE",
                        "AEORRES": "Not in dataset",
                    },
                },
                {
                    "row": 3,
                    "value": {
                        "STUDYID": "Not in dataset",
                        "DOMAIN": None,
                        "AEORRES": "Not in dataset",
                    },
                },
            ],
        }
    ]


@patch("engine.services.local_data_service.LocalDataService.get_dataset")
def test_validate_dataset_contents_against_library_metadata_variable_metadata_not_found(
    mock_get_dataset: MagicMock,
    rule_check_dataset_contents_against_library_metadata: dict,
):
    """
    Test for validating dataset contents against library metadata.

    The test checks the case when metadata for a given variable is not found.
    Expected result is an execution error saying that the metadata is not found.
    """
    mock_get_dataset.return_value = pd.DataFrame()

    # save library metadata to cache
    standard: str = "sdtmig"
    standard_version: str = "3-1-2"
    variables_metadata: dict = {
        "STUDYID": {
            "core": "Req",
        },
        "AEORRES": {
            "core": "Exp",
        },
    }
    cache = InMemoryCacheService()
    cache.add(
        get_library_variables_metadata_cache_key(standard, standard_version),
        variables_metadata,
    )

    # run the validation and check result
    rules_engine = RulesEngine(
        cache=cache,
        standard=standard,
        standard_version=standard_version,
    )
    validation_result: List[dict] = rules_engine.validate_single_rule(
        rule=rule_check_dataset_contents_against_library_metadata,
        dataset_path="study_id/data_bundle_id/filename",
        datasets=[
            {
                "domain": "AE",
                "filename": "ae.xpt",
            },
        ],
        dataset_domain="AE",
    )
    assert validation_result == [
        {
            "executionStatus": "execution_error",
            "domain": "AE",
            "variables": [],
            "message": "rule execution error",
            "errors": [
                {
                    "error": VariableMetadataNotFoundError.description,
                    "message": "Metadata for variable DOMAIN is not found in CDISC Library",
                }
            ],
        }
    ]


@patch("engine.services.local_data_service.LocalDataService.get_dataset")
def test_validate_single_rule_operation_dataset_larger_than_target_dataset(
    mock_get_dataset: MagicMock,
    rule_distinct_operation_is_not_contained_by: dict,
):
    """
    Unit test for the rules engine that ensures that
    if the operation result is longer than the target dataset
    the validation is being performed correctly.
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
            "IESEQ": [
                1,
            ],
            "IETEST": [
                "Matching value",
            ],
        }
    )
    operation_result_dataset = pd.DataFrame.from_dict(
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

    path_to_dataset_map: dict = {
        "study_id/data_bundle_id/ie.xpt": target_dataset,
        "study_id/data_bundle_id/ti.xpt": operation_result_dataset,
    }
    mock_get_dataset.side_effect = lambda dataset_name: path_to_dataset_map[
        dataset_name
    ]
    validation_result: List[dict] = RulesEngine().validate_single_rule(
        rule=rule_distinct_operation_is_not_contained_by,
        dataset_path="study_id/data_bundle_id/ie.xpt",
        datasets=[
            {
                "domain": "IE",
                "filename": "ie.xpt",
            },
            {
                "domain": "TI",
                "filename": "ti.xpt",
            },
        ],
        dataset_domain="IE",
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "domain": "IE",
            "variables": [],
            "message": None,
            "errors": [],
        }
    ]


@patch("engine.services.local_data_service.LocalDataService.get_dataset")
@patch("engine.services.local_data_service.LocalDataService.get_dataset_metadata")
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
    mock_get_dataset_metadata.return_value = pd.DataFrame.from_dict(
        {
            "dataset_name": [
                "SUPPEC",
            ],
        }
    )

    # create a dataset
    dataset = pd.DataFrame.from_dict(
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
    mock_get_dataset.return_value = dataset

    # run validation
    validation_result: List[dict] = RulesEngine().validate_single_rule(
        rule=rule_equal_to_with_extract_metadata_operation,
        dataset_path="study_id/data_bundle_id/suppec.xpt",
        datasets=[
            {
                "domain": "SUPPEC",
                "filename": "suppec.xpt",
            },
        ],
        dataset_domain="SUPPEC",
    )
    assert validation_result == [
        {
            "executionStatus": "success",
            "domain": "SUPPEC",
            "variables": [
                "RDOMAIN",
            ],
            "message": RuleProcessor.extract_message_from_rule(
                rule_equal_to_with_extract_metadata_operation
            ),
            "errors": [
                {
                    "row": 1,
                    "value": {
                        "RDOMAIN": "EC",
                    },
                },
                {
                    "row": 2,
                    "value": {
                        "RDOMAIN": "EC",
                    },
                },
                {
                    "row": 3,
                    "value": {
                        "RDOMAIN": "EC",
                    },
                },
            ],
        }
    ]


def test_is_custom_domain():
    """
    Unit test for RulesEngine.is_custom_domain() function.
    """
    cache = InMemoryCacheService()
    standard = "sdtmig"
    standard_version = "3-1-2"
    cache_key = get_standard_details_cache_key(standard, standard_version)
    cache.add(cache_key, {
        "domains": {
            "AE",
            "EC",
            "DM",
        }
    })
    engine = RulesEngine(cache=cache, standard=standard, standard_version=standard_version)
    assert engine.is_custom_domain("AP")
    assert not engine.is_custom_domain("AE")
