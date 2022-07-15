from typing import Set, List
from unittest import mock

from engine.models.rule_conditions import ConditionCompositeFactory
from engine.services.cache.in_memory_cache_service import InMemoryCacheService
from engine.utilities.rule_processor import RuleProcessor
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
from conftest import mock_data_service


@pytest.mark.parametrize(
    "domain, rule_metadata, outcome",
    [
        ("AE", {"domains": {"Include": ["AE"]}}, True),
        ("AE", {"domains": {"Include": ["All"]}}, True),
        ("AE", {"domains": {"Exclude": ["AE"]}}, False),
        ("AE", {"domains": {"Exclude": ["All"]}}, False),
        ("AE", {"domains": {"Include": ["TV"]}}, False),
        ("SUPPAE", {"domains": {"Exclude": ["SUPP--"]}}, False),
        ("SUPPAE", {"domains": {"Exclude": ["All"]}}, False),
        ("SUPPAE", {"domains": {"Include": ["SUPP--"]}}, True),
        ("SUPPAE", {"domains": {"Include": ["All"]}}, True),
        ("APTE", {"domains": {"Exclude": ["AP--"]}}, False),
        ("APTE", {"domains": {"Exclude": ["All"]}}, False),
        ("APTE", {"domains": {"Include": ["AP--"]}}, True),
        ("APTE", {"domains": {"Include": ["All"]}}, True),
        ("APRELSUB", {"domains": {"Exclude": ["APRELSUB"]}}, False),
        ("APRELSUB", {"domains": {"Exclude": ["All"]}}, False),
        ("APRELSUB", {"domains": {"Include": ["APRELSUB"]}}, True),
        ("APRELSUB", {"domains": {"Include": ["All"]}}, True),
        ("APFASU", {"domains": {"Exclude": ["APFA--"]}}, False),
        ("APFASU", {"domains": {"Exclude": ["All"]}}, False),
        ("APFASU", {"domains": {"Include": ["APFA--"]}}, True),
        ("APFASU", {"domains": {"Include": ["All"]}}, True),
    ],
)
def test_rule_applies_to_domain(mock_data_service, domain, rule_metadata, outcome):
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    assert processor.rule_applies_to_domain(domain, rule_metadata, False) == outcome


@pytest.mark.parametrize(
    "rule_domains, expected_results",
    [
        (
            {
                "Include": ["All"],
                "include_split_datasets": True,  # Includes all
            },
            [True, True, True, True, True, True],
        ),
        (
            {},  # Domains are included by default
            [True, True, True, True, True, True],
        ),
        (
            {
                "include_split_datasets": True,  # Only include split datasets
            },
            [
                False,
                False,
                True,
                True,
                True,
                True,
            ],
        ),
        (
            {
                "Include": ["AE"],
                "include_split_datasets": True,
            },
            [
                True,
                False,
                True,
                True,
                True,
                True,
            ],
        ),
        (
            # Only run on split datasets except SUPP
            {
                "Exclude": ["SUPP--"],
                "include_split_datasets": True,
            },
            [
                False,
                False,
                True,
                True,
                False,
                False,
            ],
        ),
        (
            {
                "Include": [
                    "EC",
                ],
                "Exclude": ["SUPP--"],
                "include_split_datasets": True,
            },
            [
                False,
                True,
                True,
                True,
                False,
                False,
            ],
        ),
        (
            {
                "Include": [
                    "EC",
                    "QS",
                ],
                "include_split_datasets": False,
            },
            [
                False,
                True,
                False,
                False,
                False,
                False,
            ],
        ),
        (
            {
                "Include": [
                    "EC",
                    "QS",
                ],
                "include_split_datasets": True,
            },
            [
                False,
                True,
                True,
                True,
                True,
                True,
            ],
        ),
        (
            {"Include": ["QS"]},
            [
                False,
                False,
                True,
                True,
                False,
                False,
            ],
        ),
        (
            {
                "Exclude": ["QS", "SUPPQS"],
                "include_split_datasets": True,
            },
            [
                False,
                False,
                False,
                False,
                False,
                False,
            ],
        ),
        (
            {"Include": ["EC"]},
            [False, True, False, False, False, False],
        ),
    ],
)
def test_rule_applies_to_domain_split_datasets(
    mock_data_service, rule_domains: dict, expected_results: List[bool]
):
    rule = {"domains": rule_domains}
    domains: List[dict] = [
        {"domain": "AE", "is_split": False},
        {"domain": "EC", "is_split": False},
        {"domain": "QS", "is_split": True},  # Two datasets with QS domain
        {"domain": "QS", "is_split": True},
        {"domain": "SUPPQS", "is_split": True},  # Two datasets with SUPPQS domain
        {"domain": "SUPPQS", "is_split": True},
    ]
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    results = [
        processor.rule_applies_to_domain(domain["domain"], rule, domain["is_split"])
        for domain in domains
    ]
    assert results == expected_results


@pytest.mark.parametrize(
    "datasets, domain, rule_metadata, data, class_name, outcome",
    [
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            "AE",
            {"classes": {"Exclude": ["Events"]}},
            {"AETERM": [10, 20]},
            "Events",
            False,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            "AE",
            {"classes": {"Exclude": ["Interventions"]}},
            {"AETRT": [10, 20]},
            "Interventions",
            False,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            "AE",
            {"classes": {"Exclude": ["Findings"]}},
            {"AETESTCD": [10, 20]},
            "Findings",
            False,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt"}],
            {"DOMAIN": "APTE"},
            {"classes": {"Exclude": ["Associated Persons"]}},
            {"APTE": [10, 20]},
            "Associated Persons",
            False,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            "AE",
            {"classes": {"Exclude": ["Findings", "Interventions"]}},
            {"AETERM": [10, 20]},
            "Events",
            True,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            "AE",
            {"classes": {"Exclude": ["Events", "Findings"]}},
            {"AETRT": [10, 20]},
            "Interventions",
            True,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt"}],
            "AE",
            {"classes": {"Exclude": ["Events", "Interventions"]}},
            {"AETESTCD": [10, 20]},
            "Findings",
            True,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt"}],
            {"DOMAIN": "APTE"},
            {"classes": {"Exclude": ["Events", "Interventions"]}},
            {"APTE": [10, 20]},
            "Associated Persons",
            True,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt"}],
            {"DOMAIN": "APTE"},
            {"classes": {"Exclude": ["Interventions", "Findings"]}},
            {"APTE": [10, 20]},
            "Associated Persons",
            True,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt"}],
            {"DOMAIN": "APTE"},
            {"classes": {"Exclude": ["Events", "Findings"]}},
            {"APTE": [10, 20]},
            "Associated Persons",
            True,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt"}],
            {"DOMAIN": "APTE"},
            {"classes": {"Exclude": ["Events", "Interventions", "Findings"]}},
            {"APTE": [10, 20]},
            "Associated Persons",
            True,
        ),
    ],
)
def test_rule_applies_to_class(
    mock_data_service,
    datasets,
    domain,
    rule_metadata,
    data,
    class_name,
    outcome,
):
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    dataset_mock = pd.DataFrame.from_dict(data)
    mock_data_service.get_dataset_class.return_value = class_name
    with patch(
        "engine.services.blob_data_service.BlobDataService.get_dataset",
        return_value=dataset_mock,
    ):
        assert (
            processor.rule_applies_to_class(rule_metadata, domain, datasets) == outcome
        )


def test_perform_rule_operation(mock_data_service):
    conditions = {
        "any": [
            {
                "name": "check_value",
                "params": {"target": "AESTDY"},
                "operator": "less_than",
                "value": "$max_aestdy",
            },
            {
                "name": "check_value",
                "params": {"target": "AESTDY"},
                "operator": "greater_than",
                "value": "$min_aestdy",
            },
            {
                "name": "check_value",
                "params": {"target": "AESTDY"},
                "operator": "greater_than",
                "value": "$avg_aestdy",
            },
            {
                "name": "check_value",
                "params": {"target": "AESTDY"},
                "operator": "is_contained_by",
                "value": "$unique_aestdy",
            },
        ]
    }
    rule = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions),
        "operations": [
            {"operator": "max", "domain": "AE", "name": "AESTDY", "id": "$max_aestdy"},
            {"operator": "min", "domain": "AE", "name": "AESTDY", "id": "$min_aestdy"},
            {
                "operator": "mean",
                "domain": "AE",
                "name": "AESTDY",
                "id": "$avg_aestdy",
            },
            {
                "operator": "distinct",
                "domain": "AE",
                "name": "AESTDY",
                "id": "$unique_aestdy",
            },
        ],
    }
    df = pd.DataFrame.from_dict({"AESTDY": [11, 12, 40, 59, 59], "DOMAIN": ["AE", "AE", "AE", "AE", "AE"]})
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    with patch(
        "engine.services.blob_data_service.BlobDataService.get_dataset", return_value=df
    ):
        result = processor.perform_rule_operations(
            rule, df, "AE", [{"domain": "AE", "filename": "ae.xpt"}], "test/", standard="sdtmig",
            standard_version="3-1-2"
        )
        assert "$avg_aestdy" in result
        assert "$unique_aestdy" in result
        assert "$max_aestdy" in result
        assert "$min_aestdy" in result
        assert result["$max_aestdy"][0] == df["AESTDY"].max()
        assert result["$min_aestdy"][0] == df["AESTDY"].min()
        assert result["$avg_aestdy"][0] == df["AESTDY"].mean()
        assert result["$unique_aestdy"].equals(pd.Series([{11, 12, 40, 59}] * len(df)))


def test_perform_rule_operation_with_grouping(mock_data_service):
    conditions = {
        "all": [
            {
                "name": "check_value",
                "params": {"target": "AESTDY"},
                "operator": "less_than",
                "value": "$max_aestdy",
            }
        ]
    }
    rule = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions),
        "actions": [
            {
                "name": "generate_record_message",
                "params": {
                    "message": "Value for AESTDY less than the maximum EC.ECDOSE value: $max_aestdy",
                    "target": "AESTDY",
                },
            }
        ],
        "operations": [
            {
                "operator": "max",
                "domain": "AE",
                "name": "AESTDY",
                "group": ["USUBJID"],
                "id": "$max_aestdy",
            },
            {
                "operator": "min",
                "domain": "AE",
                "name": "AESTDY",
                "group": ["USUBJID"],
                "id": "$min_aestdy",
            },
            {
                "operator": "mean",
                "domain": "AE",
                "name": "AESTDY",
                "group": ["USUBJID"],
                "id": "$avg_aestdy",
            },
            {
                "operator": "distinct",
                "domain": "AE",
                "name": "AESTDY",
                "id": "$unique_aestdy",
                "group": ["USUBJID"],
            },
        ],
    }
    df = pd.DataFrame.from_dict(
        {"AESTDY": [10, 11, 40, 59], "USUBJID": [1, 200, 1, 200], "AESEQ": [1, 2, 3, 4], "DOMAIN": ["AE", "AE", "AE", "AE"]}
    )
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    with patch(
        "engine.services.blob_data_service.BlobDataService.get_dataset", return_value=df
    ):
        data = processor.perform_rule_operations(
            rule, df, "AE", [{"domain": "AE", "filename": "ae.xpt"}], "test/", standard="sdtmig",
            standard_version="3-1-2"
        )
        assert "$avg_aestdy" in data
        assert data["$avg_aestdy"].values.tolist() == [25, 35, 25, 35]
        assert "$max_aestdy" in data
        assert data["$max_aestdy"].values.tolist() == [40, 59, 40, 59]
        assert "$min_aestdy" in data
        assert data["$min_aestdy"].values.tolist() == [10, 11, 10, 11]
        assert data[["USUBJID", "$unique_aestdy"]].equals(
            pd.DataFrame.from_dict(
                {
                    "USUBJID": [
                        1,
                        200,
                        1,
                        200,
                    ],
                    "$unique_aestdy": [
                        {
                            10,
                            40,
                        },
                        {
                            11,
                            59,
                        },
                        {
                            10,
                            40,
                        },
                        {
                            11,
                            59,
                        },
                    ],
                }
            )
        )


def test_perform_rule_operation_with_multi_key_grouping(mock_data_service):
    conditions = {
        "all": [
            {
                "name": "check_value",
                "params": {"target": "AESTDY"},
                "operator": "less_than",
                "value": "$max_aestdy",
            }
        ]
    }
    rule = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions),
        "actions": [
            {
                "name": "generate_record_message",
                "params": {
                    "message": "Value for AESTDY less than the maximum EC.ECDOSE value: $max_aestdy",
                    "target": "AESTDY",
                },
            }
        ],
        "operations": [
            {
                "operator": "max",
                "domain": "AE",
                "name": "AESTDY",
                "group": ["USUBJID", "STUDYID"],
                "id": "$max_aestdy",
            },
            {
                "operator": "min",
                "domain": "AE",
                "name": "AESTDY",
                "group": ["USUBJID", "STUDYID"],
                "id": "$min_aestdy",
            },
            {
                "operator": "mean",
                "domain": "AE",
                "name": "AESTDY",
                "group": ["USUBJID", "STUDYID"],
                "id": "$avg_aestdy",
            },
        ],
    }
    df = pd.DataFrame.from_dict(
        {
            "AESTDY": [10, 11, 40, 59, 30, 112],
            "USUBJID": [1, 200, 1, 200, 200, 1],
            "DOMAIN": ["AE", "AE", "AE", "AE", "AE", "AE"],
            "STUDYID": ["A", "A", "A", "A", "B", "B"],
        }
    )
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    with patch(
        "engine.services.blob_data_service.BlobDataService.get_dataset", return_value=df
    ):
        data = processor.perform_rule_operations(
            rule, df, "AE", [{"domain": "AE", "filename": "ae.xpt"}], "test/", standard="sdtmig",
            standard_version="3-1-2"
        )
        assert "$avg_aestdy" in data
        assert data["$avg_aestdy"].values.tolist() == [25, 35, 25, 35, 30, 112]
        assert "$max_aestdy" in data
        assert data["$max_aestdy"].values.tolist() == [40, 59, 40, 59, 30, 112]
        assert "$min_aestdy" in data
        assert data["$min_aestdy"].values.tolist() == [10, 11, 10, 11, 30, 112]


def test_perform_rule_operation_with_null_operations(mock_data_service):
    conditions = {
        "all": [
            {
                "name": "check_value",
                "params": {"target": "AESTDY"},
                "operator": "less_than",
                "value": "$max_aestdy",
            }
        ]
    }
    rule = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions),
        "actions": [
            {
                "name": "generate_record_message",
                "params": {
                    "message": "Value for AESTDY less than the maximum EC.ECDOSE value: $max_aestdy",
                    "target": "AESTDY",
                },
            }
        ],
        "operations": None,
    }
    df = pd.DataFrame.from_dict(
        {"AESTDY": [11, 12, 40, 59], "USUBJID": [1, 200, 1, 200]}
    )
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    new_data = processor.perform_rule_operations(
        rule, df, "AE", [{"domain": "AE", "filename": "ae.xpt"}], "test/", standard="sdtmig",
            standard_version="3-1-2"
    )
    assert df.equals(new_data)


@patch("engine.services.local_data_service.LocalDataService.get_dataset_metadata")
def test_perform_extract_metadata_operation(
    mock_get_dataset_metadata: MagicMock,
    rule_equal_to_with_extract_metadata_operation: dict,
):
    """
    Unit test for extract_metadata operation.
    Expected behavior is that the result of the operation
    will be added to the given dataframe.
    """
    # mock download of dataset metadata
    mock_get_dataset_metadata.return_value = pd.DataFrame.from_dict(
        {
            "dataset_name": [
                "SUPPEC",
            ],
        }
    )

    # call rule processor
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
    
    mock = MagicMock()
    mock.get_dataset.return_value = dataset
    mock.get_dataset_metadata.return_value = pd.DataFrame.from_dict(
        {
            "dataset_name": [
                "SUPPEC",
            ],
        }
    )
    processor = RuleProcessor(mock, InMemoryCacheService())
    dataset_after_operation = processor.perform_rule_operations(
        rule=rule_equal_to_with_extract_metadata_operation,
        dataset=dataset,
        domain="SUPPEC",
        datasets=[{"domain": "SUPPEC", "filename": "suppec.xpt"}],
        dataset_path="study/data_bundle/suppec.xpt",
        standard="sdtmig",
        standard_version="3-1-2"
    )

    # check result
    expected_dataset = dataset.copy()
    expected_dataset["$dataset_name"] = [
        "SUPPEC",
        "SUPPEC",
        "SUPPEC",
    ]
    print(dataset_after_operation)
    print(expected_dataset)
    assert dataset_after_operation.equals(expected_dataset)


def test_add_comparator_to_conditions(mock_data_service):
    conditions = {
        "all": [
            {"value": {"target": "dataset_location"}},
            {"value": {"target": "dataset_name"}},
        ]
    }
    rule: dict = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions)
    }
    comparator: dict = {
        "dataset_name": "AE",
        "dataset_label": "Adverse Events",
        "dataset_location": "ae.xpt",
    }
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    processor.add_comparator_to_rule_conditions(rule, comparator)
    assert rule["conditions"].to_dict() == {
        "all": [
            {
                "value": {
                    "target": "dataset_location",
                    "comparator": "ae.xpt",
                }
            },
            {
                "value": {
                    "target": "dataset_name",
                    "comparator": "AE",
                }
            },
        ]
    }


def test_add_comparator_to_conditions_nested_conditions(mock_data_service):
    """
    Unit test for function add_comparator_to_conditions.
    Ensuring that comparator is added to nested conditions as well.
    """
    conditions = {
        "all": [
            {
                "any": [
                    {"value": {"target": "dataset_name"}},
                    {"value": {"target": "dataset_label"}},
                    {"all": [{"value": {"target": "dataset_location"}}]},
                ]
            },
            {"value": {"target": "dataset_location"}},
        ]
    }
    rule: dict = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions)
    }
    comparator: dict = {
        "dataset_name": "AE",
        "dataset_label": "Adverse Events",
        "dataset_location": "ae.xpt",
    }
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    processor.add_comparator_to_rule_conditions(rule, comparator=comparator)
    assert rule["conditions"].to_dict() == {
        "all": [
            {
                "any": [
                    {
                        "value": {
                            "target": "dataset_name",
                            "comparator": "AE",
                        }
                    },
                    {
                        "value": {
                            "target": "dataset_label",
                            "comparator": "Adverse Events",
                        }
                    },
                    {
                        "all": [
                            {
                                "value": {
                                    "target": "dataset_location",
                                    "comparator": "ae.xpt",
                                }
                            }
                        ]
                    },
                ]
            },
            {
                "value": {
                    "target": "dataset_location",
                    "comparator": "ae.xpt",
                }
            },
        ]
    }


def test_add_operator_to_conditions(mock_data_service):
    """
    Unit test for add_operator_to_rule_conditions method.
    Checks nested conditions as well.
    """
    conditions = {
        "all": [
            {"name": "get_dataset", "value": {"target": "STUDYID"}},
            {"name": "get_dataset", "value": {"target": "DOMAIN"}},
            {
                "any": [
                    {"name": "get_dataset", "value": {"target": "--SEQ"}},
                ],
            },
        ]
    }
    rule = {"conditions": ConditionCompositeFactory.get_condition_composite(conditions)}
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    target_to_operator_map: dict = {
        "STUDYID": "equal_to",
        "DOMAIN": [
            "less_than",
            "not_empty",
        ],
        "AESEQ": "empty",
    }
    processor.add_operator_to_rule_conditions(rule, target_to_operator_map, "AE")
    assert rule["conditions"].to_dict() == {
        "all": [
            {
                "name": "get_dataset",
                "operator": "equal_to",
                "value": {
                    "target": "STUDYID",
                },
            },
            {
                "any": [
                    {
                        "name": "get_dataset",
                        "operator": "less_than",
                        "value": {"target": "DOMAIN"},
                    },
                    {
                        "name": "get_dataset",
                        "operator": "not_empty",
                        "value": {"target": "DOMAIN"},
                    },
                ]
            },
            {
                "any": [
                    {
                        "name": "get_dataset",
                        "operator": "empty",
                        "value": {
                            "target": "--SEQ",
                        },
                    },
                ],
            },
        ]
    }


def test_extract_target_names_from_rule():
    conditions = {
        "any": [
            {"value": {"target": "AESTDY"}},
            {"value": {"target": "USUBJID"}},
            {"all": [{"value": {"target": "TARGET"}}]},
            {"value": {"target": "AESTDY"}},
        ]
    }
    rule: dict = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions),
    }
    target_names: Set[str] = RuleProcessor.extract_target_names_from_rule(
        rule,
        "AE",
        [
            "AESTDY",
            "USUBJID",
            "TARGET",
        ],
    )
    assert target_names == {
        "AESTDY",
        "USUBJID",
        "TARGET",
    }


def test_extract_target_names_from_rule_output_variables():
    conditions = {
        "any": [
            {"value": {"target": "AESTDY"}},
            {"value": {"target": "USUBJID"}},
            {"all": [{"value": {"target": "TARGET"}}]},
            {"value": {"target": "AESTDY"}},
        ]
    }
    rule: dict = {
        "output_variables": ["AESTDY", "USUBJID", "TARGET"],
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions),
    }
    target_names: Set[str] = RuleProcessor.extract_target_names_from_rule(
        rule,
        "AE",
        [
            "AESTDY",
            "USUBJID",
            "TARGET",
        ],
    )
    assert target_names == {
        "AESTDY",
        "USUBJID",
        "TARGET",
    }


def test_create_list_of_conditions_for_each_target():
    """
    Unit test for create_list_of_conditions_for_each_target function.
    """
    # create a rule with conditions
    conditions = {
        "all": [
            {
                "operator": "empty",
                "name": "get_dataset",
            }
        ],
        "any": [
            {
                "operator": "non_empty",
                "name": "get_dataset",
            },
            {
                "operator": "equal_to",
                "name": "get_dataset",
                "value": {"comparator": 100},
            },
        ],
    }
    rule: dict = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions)
    }
    targets: List[str] = [
        "AESTDY",
        "AESEQ",
    ]

    # call the function
    RuleProcessor.create_list_of_conditions_for_each_target(rule, targets)

    # check that conditions have been created for each target
    assert rule["conditions"].to_dict() == {
        "all": [
            {
                "operator": "empty",
                "name": "get_dataset",
                "value": {
                    "target": "AESTDY",
                },
            },
            {
                "operator": "empty",
                "name": "get_dataset",
                "value": {
                    "target": "AESEQ",
                },
            },
        ],
        "any": [
            {
                "operator": "non_empty",
                "name": "get_dataset",
                "value": {"target": "AESTDY"},
            },
            {
                "operator": "non_empty",
                "name": "get_dataset",
                "value": {"target": "AESEQ"},
            },
            {
                "operator": "equal_to",
                "name": "get_dataset",
                "value": {"comparator": 100, "target": "AESTDY"},
            },
            {
                "operator": "equal_to",
                "name": "get_dataset",
                "value": {"comparator": 100, "target": "AESEQ"},
            },
        ],
    }


@pytest.mark.parametrize(
    "conditions",
    [
        {
            "any": [
                {
                    "value": {
                        "target": "dataset_label",
                        "comparator": "Adverse Events",
                    },
                    "operator": "equal_to",
                },
                {
                    "value": {"target": "dataset_size", "unit": "MB", "comparator": 5},
                    "operator": "less_than",
                },
            ]
        },
        {
            "any": [
                {
                    "value": {
                        "target": "dataset_label",
                        "comparator": "Adverse Events",
                    },
                    "operator": "equal_to",
                },
                {
                    "all": [
                        {
                            "value": {
                                "target": "dataset_size",
                                "unit": "MB",
                                "comparator": 5,
                            },
                            "operator": "less_than",
                        },
                    ]
                },
            ]
        },
        {
            "not": {
                "any": [
                    {
                        "value": {
                            "target": "dataset_label",
                            "comparator": "Adverse Events",
                        },
                        "operator": "equal_to",
                    },
                    {
                        "all": [
                            {
                                "value": {
                                    "target": "dataset_size",
                                    "unit": "MB",
                                    "comparator": 5,
                                },
                                "operator": "less_than",
                            },
                        ]
                    },
                ]
            }
        },
    ],
)
def test_get_size_unit_from_rule(conditions: dict):
    rule: dict = {
        "conditions": ConditionCompositeFactory.get_condition_composite(conditions),
    }
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    assert processor.get_size_unit_from_rule(rule) == "MB"
