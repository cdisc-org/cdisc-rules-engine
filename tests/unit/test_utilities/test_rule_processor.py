from typing import List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from cdisc_rules_engine.exceptions.custom_exceptions import DomainNotFoundError
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.models.rule_conditions.condition_composite import (
    ConditionComposite,
)
from cdisc_rules_engine.models.rule_conditions.single_condition import SingleCondition
from cdisc_rules_engine.services.cache.in_memory_cache_service import (
    InMemoryCacheService,
)
from cdisc_rules_engine.utilities.rule_processor import RuleProcessor
from cdisc_rules_engine.constants.rule_constants import ALL_KEYWORD
from cdisc_rules_engine.constants.classes import (
    FINDINGS,
    FINDINGS_ABOUT,
    EVENTS,
    INTERVENTIONS,
)
from cdisc_rules_engine.models.dataset import PandasDataset, DaskDataset


@pytest.mark.parametrize(
    "name, rule_metadata, outcome",
    [
        ("SQAE", {"domains": {"Exclude": ["SUPP--"]}}, False),
        ("SQAE", {"domains": {"Exclude": ["SUPP--", "SQ--"]}}, False),
        ("SQAE", {"domains": {"Include": ["SQ--"]}}, True),
        ("SQAE", {"domains": {"Exclude": [ALL_KEYWORD]}}, False),
        ("SQAE", {"domains": {"Include": ["SUPP--"]}}, True),
        ("SQAE", {"domains": {"Include": [ALL_KEYWORD]}}, True),
        ("AE", {"domains": {"Include": ["AE"]}}, True),
        ("AE", {"domains": {"Include": [ALL_KEYWORD]}}, True),
        ("AE", {"domains": {"Exclude": ["AE"]}}, False),
        ("AE", {"domains": {"Exclude": [ALL_KEYWORD]}}, False),
        ("AE", {"domains": {"Include": ["TV"]}}, False),
        ("SUPPAE", {"domains": {"Exclude": ["SUPP--"]}}, False),
        ("SUPPAE", {"domains": {"Exclude": [ALL_KEYWORD]}}, False),
        ("SUPPAE", {"domains": {"Include": ["SUPP--"]}}, True),
        ("SUPPAE", {"domains": {"Include": [ALL_KEYWORD]}}, True),
        ("APTE", {"domains": {"Exclude": ["AP--"]}}, False),
        ("APTE", {"domains": {"Exclude": [ALL_KEYWORD]}}, False),
        ("APTE", {"domains": {"Include": ["AP--"]}}, True),
        ("APTE", {"domains": {"Include": [ALL_KEYWORD]}}, True),
        ("APRELSUB", {"domains": {"Exclude": ["APRELSUB"]}}, False),
        ("APRELSUB", {"domains": {"Exclude": [ALL_KEYWORD]}}, False),
        ("APRELSUB", {"domains": {"Include": ["APRELSUB"]}}, True),
        ("APRELSUB", {"domains": {"Include": [ALL_KEYWORD]}}, True),
        ("APFASU", {"domains": {"Exclude": ["APFA--"]}}, False),
        ("APFASU", {"domains": {"Exclude": [ALL_KEYWORD]}}, False),
        ("APFASU", {"domains": {"Include": ["APFA--"]}}, True),
        ("APFASU", {"domains": {"Include": [ALL_KEYWORD]}}, True),
    ],
)
def test_rule_applies_to_domain(mock_data_service, name, rule_metadata, outcome):
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    first_record = None
    if name in ("APTE", "APFASU", "APRELSUB"):
        first_record = {"DOMAIN": name, "APID": "AP001"}
    assert (
        processor.rule_applies_to_domain(
            SDTMDatasetMetadata(name=name, first_record=first_record), rule_metadata
        )
        == outcome
    )


@pytest.mark.parametrize(
    "rule_domains, expected_results",
    [
        (
            {
                "Include": [ALL_KEYWORD],
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
        {"name": "AE", "domain": "AE"},
        {"name": "EC", "domain": "EC"},
        {"name": "QS1", "domain": "QS"},  # Two datasets with QS domain
        {"name": "QS2", "domain": "QS"},
        {
            "name": "SUPPQS1",
            "rdomain": "QS",
        },  # Two datasets with SUPPQS name
        {"name": "SUPPQS2", "rdomain": "QS"},
    ]
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    results = [
        processor.rule_applies_to_domain(
            SDTMDatasetMetadata(
                name=domain["name"],
                first_record={
                    "DOMAIN": domain.get("domain"),
                    "RDOMAIN": domain.get("rdomain"),
                },
            ),
            rule,
        )
        for domain in domains
    ]
    assert results == expected_results


@pytest.mark.parametrize(
    "datasets, rule_metadata, data, class_name, outcome",
    [
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Exclude": [EVENTS]}},
            {"AETERM": [10, 20]},
            EVENTS,
            False,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Exclude": [INTERVENTIONS]}},
            {"AETRT": [10, 20]},
            INTERVENTIONS,
            False,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Exclude": [FINDINGS]}},
            {"AETESTCD": [10, 20]},
            FINDINGS,
            False,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt", "full_path": "ap.xpt"}],
            {"classes": {"Exclude": ["ASSOCIATED PERSONS"]}},
            {"APTE": [10, 20]},
            "ASSOCIATED PERSONS",
            False,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Exclude": [FINDINGS, INTERVENTIONS]}},
            {"AETERM": [10, 20]},
            EVENTS,
            True,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Exclude": [EVENTS, FINDINGS]}},
            {"AETRT": [10, 20]},
            INTERVENTIONS,
            True,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Exclude": [EVENTS, INTERVENTIONS]}},
            {"AETESTCD": [10, 20]},
            FINDINGS,
            True,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt", "full_path": "ap.xpt"}],
            {"classes": {"Exclude": [EVENTS, INTERVENTIONS]}},
            {"APTE": [10, 20]},
            "ASSOCIATED PERSONS",
            True,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt", "full_path": "ap.xpt"}],
            {"classes": {"Exclude": [INTERVENTIONS, FINDINGS]}},
            {"APTE": [10, 20]},
            "ASSOCIATED PERSONS",
            True,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt", "full_path": "ap.xpt"}],
            {"classes": {"Exclude": [EVENTS, FINDINGS]}},
            {"APTE": [10, 20]},
            "ASSOCIATED PERSONS",
            True,
        ),
        (
            [{"domain": "APTE", "filename": "ap.xpt", "full_path": "ap.xpt"}],
            {"classes": {"Exclude": [EVENTS, INTERVENTIONS, FINDINGS]}},
            {"APTE": [10, 20]},
            "ASSOCIATED PERSONS",
            True,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Include": [FINDINGS]}},
            {"AETESTCD": [10, 20]},
            FINDINGS_ABOUT,
            True,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Exclude": [FINDINGS]}},
            {"AETESTCD": [10, 20]},
            FINDINGS_ABOUT,
            False,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Exclude": [FINDINGS_ABOUT]}},
            {"AETESTCD": [10, 20]},
            FINDINGS_ABOUT,
            False,
        ),
        (
            [{"domain": "AE", "filename": "ae.xpt", "full_path": "ae.xpt"}],
            {"classes": {"Include": [FINDINGS_ABOUT]}},
            {"AETESTCD": [10, 20]},
            FINDINGS_ABOUT,
            True,
        ),
    ],
)
def test_rule_applies_to_class(
    mock_data_service,
    datasets,
    rule_metadata,
    data,
    class_name,
    outcome,
):
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    dataset_mock = PandasDataset.from_dict(data)
    mock_data_service.get_dataset_class.return_value = class_name
    mock_data_service.get_datasets.return_value = datasets
    with patch(
        "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset",
        return_value=dataset_mock,
    ):
        assert (
            processor.rule_applies_to_class(
                rule_metadata,
                SDTMDatasetMetadata(*datasets[0]),
            )
            == outcome
        )


@pytest.mark.parametrize(
    "dataset_name, domain, rdomain, rule_use_case, use_case, standard, standard_substandard, outcome, is_custom",
    [
        # Basic use case tests - custom_domain_use_case is irrelevant for standard domains
        ("AE", "AE", None, "INDH, PROD", None, "tig", "SDTM", True, False),
        ("CM", "CM", None, "INDH", None, "tig", "SDTM", True, False),
        ("TS", "TS", None, "INDH", None, "tig", "SDTM", True, False),
        ("ES", "ES", None, "PROD", None, "tig", "SDTM", True, False),
        ("BW", "BW", None, "NONCLIN", None, "tig", "SEND", True, False),
        # Domain not in rule's use case domains
        ("ES", "ES", None, "INDH", None, "tig", "SDTM", False, False),
        ("BW", "BW", None, "INDH", None, "tig", "SEND", False, False),
        # command line use_case is ignored for standard domains
        ("ES", "ES", None, "PROD", "INDH", "tig", "SDTM", True, False),
        # ADAM tests
        ("ADAE", "ADAE", None, "ANALYSIS", None, "tig", "ADAM", True, False),
        ("ADAE", "ADAE", None, "INDH", None, "tig", "ADAM", False, False),
        # Supp tests
        ("SUPPAE", None, "AE", "INDH", None, "tig", "SDTM", True, False),
        ("SUPPQS", None, "QS", "INDH", None, "tig", "SDTM", True, False),
        ("SUPPEC", None, "EC", "INDH", None, "tig", "SDTM", True, False),
        ("SUPP--", None, "AE", "INDH", None, "tig", "SDTM", True, False),
        ("SUPPPT", None, "PT", "PROD", None, "tig", "SDTM", True, False),
        # Empty/None use cases in rule
        ("AE", "AE", None, "", None, "tig", "SDTM", False, False),
        ("AE", "AE", None, None, None, "tig", "SDTM", False, False),
        # Non-TIG standard
        ("AE", "AE", None, "INDH", None, "sdtmig", "SDTM", True, False),
        ("BW", "BW", None, "NONCLIN", None, "sendct", "SEND", True, False),
        # command line use_case ignored - AE is in INDH domains
        ("AE", "AE", None, "INDH, PROD", "SAFETY", "tig", "SDTM", True, False),
        # Custom domains (XYZ-prefixed)
        ("XY", "XY", None, "INDH", "INDH", "tig", "SDTM", True, True),
        ("XY", "XY", None, "INDH", "PROD", "tig", "SDTM", False, True),
        ("ZZ", "ZZ", None, "PROD", "PROD", "tig", "SDTM", True, True),
    ],
)
def test_rule_applies_to_use_case(
    mock_data_service,
    dataset_name,
    domain,
    rdomain,
    rule_use_case,
    use_case,
    standard,
    standard_substandard,
    outcome,
    is_custom,
):
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    rule = {"use_case": rule_use_case}
    dataset_metadata = SDTMDatasetMetadata(
        name=dataset_name,
        first_record=(
            {"DOMAIN": domain, "RDOMAIN": rdomain} if domain or rdomain else {}
        ),
    )

    with patch(
        "cdisc_rules_engine.utilities.rule_processor.is_custom_domain",
        return_value=is_custom,
    ):
        assert (
            processor.rule_applies_to_use_case(
                rule, standard, standard_substandard, dataset_metadata, use_case
            )
            == outcome
        )


def test_rule_applies_to_use_case_custom_domain_no_use_case_argument_raises(
    mock_data_service,
):
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    rule = {"use_case": "INDH"}
    dataset_metadata = SDTMDatasetMetadata(name="XY", first_record={"DOMAIN": "XY"})
    with patch(
        "cdisc_rules_engine.utilities.rule_processor.is_custom_domain",
        return_value=True,
    ):
        with pytest.raises(ValueError, match="requires a use case"):
            processor.rule_applies_to_use_case(
                rule, "tig", "SDTM", dataset_metadata, None
            )


@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_perform_rule_operation(mock_data_service, dataset_implementation):
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
    df = dataset_implementation.from_dict(
        {"AESTDY": [11, 12, 40, 59, 59], "DOMAIN": ["AE", "AE", "AE", "AE", "AE"]}
    )
    datasets_metadata = [
        SDTMDatasetMetadata(
            filename="ae.xpt",
            full_path="test/ae.xpt",
            name="AE",
            label="Adverse Events",
        )
    ]
    mock_data_service.get_dataset.return_value = df
    mock_data_service.get_datasets.return_value = datasets_metadata
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    result = processor.perform_rule_operations(
        rule,
        df,
        datasets_metadata[0],
        standard="sdtmig",
        standard_version="3-1-2",
        standard_substandard=None,
    )
    assert "$avg_aestdy" in result
    assert "$unique_aestdy" in result
    assert "$max_aestdy" in result
    assert "$min_aestdy" in result
    assert result["$max_aestdy"][0] == df["AESTDY"].max()
    assert result["$min_aestdy"][0] == df["AESTDY"].min()
    assert result["$avg_aestdy"][0] == df["AESTDY"].mean()
    assert result["$unique_aestdy"].equals(pd.Series([[11, 12, 40, 59]] * len(df)))


@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_perform_rule_operation_with_grouping(
    mock_data_service, dataset_implementation
):
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
                    "message": "Value for AESTDY less than the "
                    "maximum EC.ECDOSE value: $max_aestdy",
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
    df = dataset_implementation.from_dict(
        {
            "AESTDY": [10, 11, 40, 59],
            "USUBJID": [1, 200, 1, 200],
            "AESEQ": [1, 2, 3, 4],
            "DOMAIN": ["AE", "AE", "AE", "AE"],
        }
    )

    datasets_metadata = [
        SDTMDatasetMetadata(
            filename="ae.xpt",
            full_path="test/ae.xpt",
            name="AE",
            label="Adverse Events",
        )
    ]

    mock_data_service.get_dataset.return_value = df
    mock_data_service.get_datasets.return_value = datasets_metadata
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    data = processor.perform_rule_operations(
        rule,
        df,
        datasets_metadata[0],
        standard="sdtmig",
        standard_version="3-1-2",
        standard_substandard=None,
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
                    [
                        10,
                        40,
                    ],
                    [
                        11,
                        59,
                    ],
                    [
                        10,
                        40,
                    ],
                    [
                        11,
                        59,
                    ],
                ],
            }
        )
    )


@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_perform_rule_operation_with_multi_key_grouping(
    mock_data_service, dataset_implementation
):
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
                    "message": "Value for AESTDY less than the maximum"
                    "EC.ECDOSE value: $max_aestdy",
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
    df = dataset_implementation.from_dict(
        {
            "AESTDY": [10, 11, 40, 59, 30, 112],
            "USUBJID": [1, 200, 1, 200, 200, 1],
            "DOMAIN": ["AE", "AE", "AE", "AE", "AE", "AE"],
            "STUDYID": ["A", "A", "A", "A", "B", "B"],
        }
    )

    datasets_metadata = [
        SDTMDatasetMetadata(
            filename="ae.xpt",
            full_path="test/ae.xpt",
            name="AE",
            label="Adverse Events",
        )
    ]

    mock_data_service.get_dataset.return_value = df
    mock_data_service.get_datasets.return_value = datasets_metadata
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    data = processor.perform_rule_operations(
        rule,
        df,
        datasets_metadata[0],
        standard="sdtmig",
        standard_version="3-1-2",
        standard_substandard=None,
    )
    assert "$avg_aestdy" in data
    assert data["$avg_aestdy"].values.tolist() == [25, 35, 25, 35, 30, 112]
    assert "$max_aestdy" in data
    assert data["$max_aestdy"].values.tolist() == [40, 59, 40, 59, 30, 112]
    assert "$min_aestdy" in data
    assert data["$min_aestdy"].values.tolist() == [10, 11, 10, 11, 30, 112]


@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_perform_rule_operation_with_null_operations(
    mock_data_service, dataset_implementation
):
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
                    "message": "Value for AESTDY less than the "
                    "maximum EC.ECDOSE value: $max_aestdy",
                    "target": "AESTDY",
                },
            }
        ],
        "operations": None,
    }
    df = dataset_implementation.from_dict(
        {"AESTDY": [11, 12, 40, 59], "USUBJID": [1, 200, 1, 200]}
    )
    datasets_metadata = [
        SDTMDatasetMetadata(
            filename="ae.xpt",
            full_path="test/ae.xpt",
            name="AE",
            label="Adverse Events",
        )
    ]
    mock_data_service.get_datasets.return_value = datasets_metadata
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    new_data = processor.perform_rule_operations(
        rule,
        df,
        datasets_metadata[0],
        standard="sdtmig",
        standard_version="3-1-2",
        standard_substandard=None,
    )
    assert df.equals(new_data)


@patch(
    "cdisc_rules_engine.services.data_services.LocalDataService.get_dataset_metadata"
)
@pytest.mark.parametrize("dataset_implementation", [PandasDataset, DaskDataset])
def test_perform_extract_metadata_operation(
    mock_get_dataset_metadata: MagicMock,
    dataset_implementation,
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
    dataset = dataset_implementation.from_dict(
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
    datasets_metadata = [
        SDTMDatasetMetadata(
            name="SUPPEC",
            first_record={"RDOMAIN": "EC"},
            filename="suppec.xpt",
            full_path="study/data_bundle/suppec.xpt",
        )
    ]
    mock = MagicMock()
    mock.get_dataset.return_value = dataset
    mock.get_dataset_metadata.return_value = dataset_implementation.from_dict(
        {
            "dataset_name": [
                "SUPPEC",
            ],
        }
    )
    mock.get_datasets.return_value = datasets_metadata
    processor = RuleProcessor(mock, InMemoryCacheService())
    dataset_after_operation = processor.perform_rule_operations(
        rule=rule_equal_to_with_extract_metadata_operation,
        dataset=dataset,
        dataset_metadata=datasets_metadata[0],
        standard="sdtmig",
        standard_version="3-1-2",
        standard_substandard=None,
    )

    # check result
    expected_dataset = dataset.copy()
    expected_dataset["$dataset_name"] = [
        "SUPPEC",
        "SUPPEC",
        "SUPPEC",
    ]
    assert dataset_after_operation.equals(expected_dataset)


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
    target_names: List[str] = RuleProcessor.extract_target_names_from_rule(
        rule,
        "AE",
        [
            "AESTDY",
            "USUBJID",
            "TARGET",
        ],
    )
    assert target_names == [
        "AESTDY",
        "USUBJID",
        "TARGET",
    ]


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
    target_names: List[str] = RuleProcessor.extract_target_names_from_rule(
        rule,
        "AE",
        [
            "AESTDY",
            "USUBJID",
            "TARGET",
        ],
    )
    assert target_names == [
        "AESTDY",
        "USUBJID",
        "TARGET",
    ]


def test_duplicate_for_targets():
    """
    Unit test for ConditionComposite.add_variable_condtions method.
    Tests that conditions that need to be duplicated are.
    """
    composite = ConditionComposite()
    single_condition_1 = SingleCondition(
        {
            "name": "get_dataset",
            "operator": "equal_to",
            "value": {
                "comparator": "TEST",
            },
        }
    )
    composite.add_conditions("all", [single_condition_1])
    targets = ["AESTDY", "AESCAT", "AEWWWR"]
    duplicated_conditions = RuleProcessor.duplicate_conditions_for_all_targets(
        composite, targets
    )
    composite.set_conditions(duplicated_conditions)
    items = composite.items()
    check = items[0]
    assert len(check[1]) == 3
    assert check[0] == "all"
    for target in targets:
        # Assert there is one condition for each target in the targets list
        assert (
            len([cond for cond in check[1] if cond["value"]["target"] == target]) == 1
        )


def test_operation_nonexistent_domain_raises_error(mock_data_service):
    df = PandasDataset.from_dict({"DOMAIN": ["LB", "LB"], "LBSEQ": [1, 2]})
    rule = {
        "operations": [
            {"operator": "distinct", "domain": "AE", "name": "AESEQ", "id": "$ae_ids"}
        ]
    }
    processor = RuleProcessor(mock_data_service, InMemoryCacheService())
    datasets_metadata = [
        SDTMDatasetMetadata(
            name="LB",
            filename="lb.xpt",
            first_record={"DOMAIN": "LB"},
            full_path="lb.xpt",
        )
    ]
    with pytest.raises(DomainNotFoundError) as exc_info:
        processor.perform_rule_operations(
            rule=rule,
            dataset=df.copy(),
            dataset_metadata=datasets_metadata[0],
            datasets=datasets_metadata,
            standard="sdtmig",
            standard_version="3-1-2",
            standard_substandard=None,
        )
    error_message = str(exc_info.value)
    assert (
        "Failed to execute rule operation. Domain AE does not exist. Operation: distinct, Target: AESEQ, Core ID: None"
        == error_message
    )
