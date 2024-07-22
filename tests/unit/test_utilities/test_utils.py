import pytest
from unittest.mock import patch
from cdisc_rules_engine.utilities.utils import (
    is_split_dataset,
    is_supp_dataset,
    get_corresponding_datasets,
)


mock_datasets = [
    {"filename": "SS11.xpt", "domain": "SS"},
    {"filename": "SS12.xpt", "domain": "SS"},
]


@patch(
    "cdisc_rules_engine.utilities.utils.get_corresponding_datasets",
    return_value=mock_datasets,
)
def test_is_split_dataset_from_file(mock_get_corresponding_datasets):
    domain = "SS"
    result = is_split_dataset(mock_datasets, domain)
    assert result


datasets_tests = [
    (
        [
            {"filename": "suppSS.xpt", "domain": "SS"},
            {"filename": "SS.xpt", "domain": "SS"},
        ],
        True,
    ),
    (
        [
            {"filename": "SS.xpt", "domain": "SS"},
            {"filename": "SS12.xpt", "domain": "SS"},
        ],
        False,
    ),
    ([{"filename": "suppSS.xpt", "domain": "SS"}], False),
]


@pytest.mark.parametrize("mock_datasets, expected", datasets_tests)
@patch("cdisc_rules_engine.utilities.utils.get_corresponding_datasets")
def test_is_supp_dataset(mock_get_corresponding_datasets, mock_datasets, expected):
    domain = "SS"
    mock_get_corresponding_datasets.return_value = mock_datasets
    result = is_supp_dataset(mock_datasets, domain)
    assert (
        result == expected
    ), f"Expected {expected} but got {result} for datasets {mock_datasets}"


datasets = [
    {"filename": "SS.xpt", "domain": "SS"},
    {"filename": "SS12.xpt", "domain": "SS"},
    {"filename": "AE.xpt", "domain": "AE"},
    {"filename": "DD.xpt", "domain": "DD"},
    {"filename": "EC.xpt", "domain": "EC"},
    {"filename": "EX.xpt", "domain": "EX"},
    {"filename": "FA.xpt", "domain": "FA"},
    {"filename": "FT.xpt", "domain": "FT"},
    {"filename": "RS.xpt", "domain": "RS"},
    {"filename": "AB.xpt", "domain": "AB"},
    {"filename": "AB12.xpt", "domain": "AB"},
]


# Parameters for testing each domain
domain_test_cases = [
    (
        "SS",
        [
            {"filename": "SS.xpt", "domain": "SS"},
            {"filename": "SS12.xpt", "domain": "SS"},
        ],
    ),
    (
        "AB",
        [
            {"filename": "AB.xpt", "domain": "AB"},
            {"filename": "AB12.xpt", "domain": "AB"},
        ],
    ),
    ("AE", [{"filename": "AE.xpt", "domain": "AE"}]),
    ("DD", [{"filename": "DD.xpt", "domain": "DD"}]),
    ("EC", [{"filename": "EC.xpt", "domain": "EC"}]),
    ("EX", [{"filename": "EX.xpt", "domain": "EX"}]),
    ("FA", [{"filename": "FA.xpt", "domain": "FA"}]),
    ("FT", [{"filename": "FT.xpt", "domain": "FT"}]),
    ("RS", [{"filename": "RS.xpt", "domain": "RS"}]),
]


@pytest.mark.parametrize("domain, expected_datasets", domain_test_cases)
def test_get_corresponding_datasets(domain, expected_datasets):
    result_datasets = get_corresponding_datasets(datasets, domain)
    assert (
        result_datasets == expected_datasets
    ), f"The function should return only datasets matching the '{domain}' domain"
