from unittest.mock import MagicMock
from numpy import nan
import pandas as pd
import pytest
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.codelist_terms import CodelistTerms
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.exceptions.custom_exceptions import (
    MissingDataError,
    RuleExecutionError,
)


@pytest.fixture
def mock_metadata():
    return {
        "mock_package": {
            "submission_lookup": {
                "CL1": {"codelist": "C1", "term": "N/A"},
                "CL2": {"codelist": "C2", "term": "Term1"},
            },
            "C1": {
                "submissionValue": "Codelist1",
                "terms": [
                    {"conceptId": "T1", "submissionValue": "Term1"},
                    {"conceptId": "T2", "submissionValue": "Term2"},
                ],
            },
            "C2": {
                "submissionValue": "Codelist2",
                "terms": [
                    {"conceptId": "T3", "submissionValue": "Term3"},
                    {"conceptId": "T4", "submissionValue": "Term4"},
                ],
            },
        }
    }


def test_codelist_level_code(operation_params, mock_metadata):
    operation_params.codelists = ["CL1"]
    operation_params.level = "codelist"
    operation_params.returntype = "code"

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = CodelistTerms(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["C1"]


def test_codelist_level_value(operation_params, mock_metadata):
    operation_params.codelists = ["CL1"]
    operation_params.level = "codelist"
    operation_params.returntype = "value"

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = CodelistTerms(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["Codelist1"]


def test_term_level_code(operation_params, mock_metadata):
    operation_params.codelists = ["CL1"]
    operation_params.level = "term"
    operation_params.returntype = "code"

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = CodelistTerms(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["T1", "T2"]


def test_term_level_value(operation_params, mock_metadata):
    operation_params.codelists = ["CL1"]
    operation_params.level = "term"
    operation_params.returntype = "value"

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = CodelistTerms(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["Term1", "Term2"]


def test_multiple_codelists(operation_params, mock_metadata):
    operation_params.codelists = ["CL1", "CL2"]
    operation_params.level = "term"
    operation_params.returntype = "value"

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = CodelistTerms(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["Term1", "Term2", "Term3", "Term4"]


def test_missing_codelist(operation_params):
    operation_params.codelists = ["CL3"]
    operation_params.level = "codelist"
    operation_params.returntype = "code"

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = {
        "mock_package": {
            "submission_lookup": {},
            "C1": {"submissionValue": "Codelist1", "terms": []},
        }
    }

    operation = CodelistTerms(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    with pytest.raises(MissingDataError, match="Codelist 'CL3' not found in metadata"):
        operation._execute_operation()


def test_empty_terms(operation_params):
    operation_params.codelists = ["CL1"]
    operation_params.level = "term"
    operation_params.returntype = "code"

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = {
        "mock_package": {
            "submission_lookup": {"CL1": {"codelist": "C1", "term": "N/A"}},
            "C1": {"submissionValue": "Codelist1", "terms": []},
        }
    }

    operation = CodelistTerms(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == []


@pytest.mark.parametrize(
    "package_type, codelist_code, term_code, term_value, expected",
    [
        (
            "mock_package",
            "codelist_code",
            "t_code",
            None,
            ("Term1", "Term2", "Term3", nan),
        ),
        ("mock_package", "codelist_code", None, "t_value", ("T1", nan, "T3", "T4")),
        ("mock_package", "codelist_code", "t_code", "t_value", RuleExecutionError),
        ("mock_package", "C1", "t_code", None, ("Term1", "Term2", nan, nan)),
        ("mock_package", "C1", None, "t_value", ("T1", nan, nan, nan)),
        ("mock_package", "C2", "t_code", None, (nan, nan, "Term3", nan)),
        ("mock_package", "C2", None, "t_value", (nan, nan, "T3", "T4")),
        ("missing_package", "codelist_code", "t_code", None, (None, None, None, None)),
    ],
)
def test_multiple_versions(
    package_type,
    codelist_code,
    term_code,
    term_value,
    expected,
    operation_params,
    mock_metadata,
):
    operation_params.ct_package_type = package_type
    operation_params.ct_version = "version"
    operation_params.codelist_code = codelist_code
    operation_params.term_code = term_code
    operation_params.term_value = term_value
    versions = ["v1", "v2", "v1", "v2"]

    library_metadata = LibraryMetadataContainer()
    for version in versions:
        mock_metadata[f"mock_package-{version}"] = mock_metadata["mock_package"]
    library_metadata._ct_package_metadata = mock_metadata

    evaluation_dataset = PandasDataset.from_dict(
        {
            "version": versions,
            "codelist_code": ["C1", "C1", "C2", "C2"],
            "t_code": ["T1", "T2", "t3", "T9"],
            "t_value": ["Term1", "Term9", "term3", "Term4"],
        }
    )

    operation = CodelistTerms(
        operation_params,
        evaluation_dataset,
        MagicMock(),
        MagicMock(),
        library_metadata,
    )
    try:
        result = operation._execute_operation()
    except RuleExecutionError:
        result = pd.Series(RuleExecutionError)
    assert result.equals(pd.Series(expected))
