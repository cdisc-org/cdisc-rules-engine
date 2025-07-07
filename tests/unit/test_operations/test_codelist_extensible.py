from unittest.mock import MagicMock
from pytest import fixture, mark, raises
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.codelist_extensible import CodelistExtensible
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError


@fixture
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
    with raises(
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
    with raises(StopIteration):
        operation._execute_operation()


@mark.parametrize(
    "package_type, codelist_code, expected",
    [
        (
            "mock_package",
            "codelist_code",
            [True, False, None],
        ),
        (
            "mock_package",
            "C1",
            [True, True, True],
        ),
        (
            "missing_package",
            "codelist_code",
            [None, None, None],
        ),
    ],
)
def test_multiple_versions(
    operation_params, mock_metadata, package_type, codelist_code, expected
):
    operation_params.ct_package_type = package_type
    operation_params.ct_version = "version"
    operation_params.codelist_code = codelist_code
    versions = ["v1", "v2", "v3"]

    library_metadata = LibraryMetadataContainer()
    for version in versions:
        mock_metadata[f"mock_package-{version}"] = mock_metadata["mock_package"]
    library_metadata._ct_package_metadata = mock_metadata

    evaluation_dataset = PandasDataset.from_dict(
        {"version": versions, "codelist_code": ["C1", "C2", "C3"]}
    )

    operation = CodelistExtensible(
        operation_params,
        evaluation_dataset,
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result.tolist() == expected
