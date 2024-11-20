from unittest.mock import MagicMock
import pytest
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.codelist_terms import CodelistTerms
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError


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


@pytest.fixture
def operation_params():
    return OperationParams(
        dataframe=PandasDataset.from_dict({}),
        dataset_path="test_path",
        datasets={"TEST": PandasDataset.from_dict({})},
        domain="TEST",
        directory_path="test_dir",
        operation_id="test_op",
        operation_name="test_operation",
        standard="SDTM",
        standard_version="1.0",
    )


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
