from unittest.mock import MagicMock
import pytest
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.codelist_extensible import CodelistExtensible
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
                "CL3": {"codelist": "C3", "term": "N/A"},
            },
            "C1": {
                "submissionValue": "Codelist1",
                "extensible": True,
                "terms": [
                    {"conceptId": "T1", "submissionValue": "Term1"},
                    {"conceptId": "T2", "submissionValue": "Term2"},
                ],
            },
            "C2": {
                "submissionValue": "Codelist2",
                "extensible": False,
                "terms": [
                    {"conceptId": "T3", "submissionValue": "Term3"},
                    {"conceptId": "T4", "submissionValue": "Term4"},
                ],
            },
            "C3": {"submissionValue": "Codelist3", "terms": []},
        }
    }


def test_extensible_codelist(operation_params, mock_metadata):
    """Test codelist that is extensible"""
    operation_params.codelist = "CL1"
    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata
    operation = CodelistExtensible(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )
    result = operation._execute_operation()
    assert result is True


def test_non_extensible_codelist(operation_params, mock_metadata):
    """Test codelist that is not extensible"""
    operation_params.codelist = "CL2"
    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata
    operation = CodelistExtensible(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )
    result = operation._execute_operation()
    assert result is False


def test_codelist_without_extensible_property(operation_params, mock_metadata):
    """Test codelist that doesn't specify extensible property"""
    operation_params.codelist = "CL3"
    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata
    operation = CodelistExtensible(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )
    result = operation._execute_operation()
    assert result is None


def test_missing_codelist(operation_params):
    """Test behavior when codelist is not found"""
    operation_params.codelist = "CL_NONEXISTENT"
    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = {
        "mock_package": {
            "submission_lookup": {},
            "C1": {"submissionValue": "Codelist1", "extensible": True, "terms": []},
        }
    }
    operation = CodelistExtensible(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )
    with pytest.raises(
        MissingDataError, match="Codelist 'CL_NONEXISTENT' not found in metadata"
    ):
        operation._execute_operation()


def test_empty_metadata(operation_params):
    """Test behavior when metadata is empty"""
    operation_params.codelist = "CL1"
    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = {}
    operation = CodelistExtensible(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )
    with pytest.raises(StopIteration):
        operation._execute_operation()
