import pytest
from unittest.mock import Mock
from scripts.script_utils import (
    get_library_metadata_from_cache,
)

from cdisc_rules_engine.utilities.sdtm_utilities import (
    get_variables_metadata_from_standard,
    get_variables_metadata_from_standard_model,
)


@pytest.fixture
def library_metadata():
    args = Mock()
    args.cache = "resources/cache"
    args.standard = "sdtmig"
    args.version = "3-4"
    args.substandard = None
    args.custom_standard = False
    args.define_xml_path = None
    args.controlled_terminology_package = []
    return get_library_metadata_from_cache(args)


@pytest.fixture
def mock_data_service():
    """Mock data service for tests that require it."""
    return Mock()


@pytest.fixture
def mock_datasets():
    """Mock datasets metadata for tests."""
    return []


def test_standard_domain_ae(library_metadata):
    variables = get_variables_metadata_from_standard("AE", library_metadata)
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "AETERM" for var in variables)
    assert any(var["name"] == "AESTDTC" for var in variables)


def test_standard_domain_dm(library_metadata):
    variables = get_variables_metadata_from_standard("DM", library_metadata)
    assert any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "AGE" for var in variables)
    assert any(var["name"] == "SEX" for var in variables)


def test_findings_domain_lb(library_metadata):
    variables = get_variables_metadata_from_standard("LB", library_metadata)
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "LBTEST" for var in variables)
    assert any(var["name"] == "LBORRES" for var in variables)


def test_supp_domain(library_metadata):
    variables = get_variables_metadata_from_standard("SUPPAE", library_metadata)
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "QNAM" for var in variables)
    assert any(var["name"] == "QLABEL" for var in variables)


def test_sq_domain(library_metadata):
    variables = get_variables_metadata_from_standard("SQAE", library_metadata)
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "QNAM" for var in variables)
    assert any(var["name"] == "QLABEL" for var in variables)


def test_ap_domain(library_metadata):
    variables = get_variables_metadata_from_standard("APDM", library_metadata)
    assert any(var["name"] == "APID" for var in variables)
    assert not any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "RSUBJID" for var in variables)
    assert any(var["name"] == "RACE" for var in variables)
    assert any(var["name"] == "DMDY" for var in variables)


def test_findings_about_domain_fa(library_metadata):
    """Test Findings About domain includes FINDINGS class variables."""
    variables = get_variables_metadata_from_standard("FA", library_metadata)
    assert any(var["name"] == "FATEST" for var in variables)
    assert any(var["name"] == "FAOBJ" for var in variables)


# Tests for get_variables_metadata_from_standard_model
def test_findings_domain_from_model(library_metadata, mock_data_service, mock_datasets):
    mock_dataframe = Mock()

    variables = get_variables_metadata_from_standard_model(
        domain="LB",
        dataframe=mock_dataframe,
        datasets=mock_datasets,
        dataset_path="/path/to/lb.xpt",
        data_service=mock_data_service,
        library_metadata=library_metadata,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "LBTEST" for var in variables)


def test_supp_domain_from_model(library_metadata, mock_data_service, mock_datasets):
    """Test retrieving variables for SUPP domain from model."""
    mock_dataframe = Mock()

    variables = get_variables_metadata_from_standard_model(
        domain="SUPPAE",
        dataframe=mock_dataframe,
        datasets=mock_datasets,
        dataset_path="/path/to/suppae.xpt",
        data_service=mock_data_service,
        library_metadata=library_metadata,
    )
    assert any(var["name"] == "RDOMAIN" for var in variables)
    assert any(var["name"] == "IDVAR" for var in variables)


def test_ap_domain_from_model(library_metadata, mock_data_service, mock_datasets):
    """Test AP domain excludes USUBJID and includes APID."""
    mock_dataframe = Mock()
    variables = get_variables_metadata_from_standard_model(
        domain="APDM",
        dataframe=mock_dataframe,
        datasets=mock_datasets,
        dataset_path="/path/to/apdm.xpt",
        data_service=mock_data_service,
        library_metadata=library_metadata,
    )
    assert not any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "APID" for var in variables)
    assert any(var["name"] == "AGE" for var in variables)
    assert any(var["name"] == "DMDY" for var in variables)
