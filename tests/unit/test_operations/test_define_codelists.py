import pytest
from unittest.mock import MagicMock
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset
from cdisc_rules_engine.operations.define_xml_extensible_codelists import (
    DefineCodelists,
)
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.exceptions.custom_exceptions import MissingDataError


@pytest.fixture
def mock_metadata():
    return {
        "extensible": {
            "CODELIST1": {"extended_values": ["EXT1", "EXT2", "EXT3"]},
            "CODELIST2": {"extended_values": ["EXT4", "EXT5"]},
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


def test_single_codelist_values(operation_params, mock_metadata):
    operation_params.codelists = ["CODELIST1"]

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = DefineCodelists(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["EXT1", "EXT2", "EXT3"]


def test_multiple_codelists_values(operation_params, mock_metadata):
    operation_params.codelists = ["CODELIST1", "CODELIST2"]

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = DefineCodelists(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["EXT1", "EXT2", "EXT3", "EXT4", "EXT5"]


def test_all_codelists(operation_params, mock_metadata):
    operation_params.codelists = ["ALL"]

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = DefineCodelists(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["EXT1", "EXT2", "EXT3", "EXT4", "EXT5"]


def test_case_insensitive_codelist_lookup(operation_params, mock_metadata):
    operation_params.codelists = ["codelist1"]

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = DefineCodelists(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    result = operation._execute_operation()
    assert result == ["EXT1", "EXT2", "EXT3"]


def test_missing_codelist(operation_params, mock_metadata):
    operation_params.codelists = ["NONEXISTENT"]

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = DefineCodelists(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    with pytest.raises(
        MissingDataError, match="Codelist 'NONEXISTENT' not found in metadata"
    ):
        operation._execute_operation()


def test_missing_codelists_parameter(operation_params, mock_metadata):
    operation_params.codelists = None

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = mock_metadata

    operation = DefineCodelists(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    with pytest.raises(
        MissingDataError, match="Codelists operation parameter not provided"
    ):
        operation._execute_operation()


def test_missing_extensible_metadata(operation_params):
    operation_params.codelists = ["CODELIST1"]

    library_metadata = LibraryMetadataContainer()
    library_metadata._ct_package_metadata = {}  # Empty metadata

    operation = DefineCodelists(
        operation_params,
        PandasDataset.from_dict({}),
        MagicMock(),
        MagicMock(),
        library_metadata,
    )

    with pytest.raises(
        MissingDataError,
        match="Parsed Extensible terms not found in library CT metadata",
    ):
        operation._execute_operation()
