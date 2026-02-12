import pytest
from unittest.mock import Mock
from scripts.script_utils import (
    get_library_metadata_from_cache,
)

from cdisc_rules_engine.utilities.sdtm_utilities import (
    get_variables_metadata_from_standard,
    get_variables_metadata_from_standard_model,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata


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
    mock_service = Mock()
    mock_service._handle_custom_domains = Mock(return_value=None)
    return mock_service


@pytest.fixture
def mock_datasets():
    """Mock datasets metadata for tests."""
    return []


@pytest.fixture
def mock_dataset():
    """Mock dataset for tests."""
    return Mock()


def test_standard_domain_ae(
    library_metadata, mock_data_service, mock_dataset, mock_datasets
):
    dataset_metadata = SDTMDatasetMetadata(name="AE", first_record={"DOMAIN": "AE"})
    variables = get_variables_metadata_from_standard(
        "AE",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/ae.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "AETERM" for var in variables)
    assert any(var["name"] == "AESTDTC" for var in variables)


def test_standard_domain_dm(
    library_metadata, mock_data_service, mock_dataset, mock_datasets
):
    dataset_metadata = SDTMDatasetMetadata(name="DM", first_record={"DOMAIN": "DM"})
    variables = get_variables_metadata_from_standard(
        "DM",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/dm.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "AGE" for var in variables)
    assert any(var["name"] == "SEX" for var in variables)


def test_findings_domain_lb(
    library_metadata, mock_data_service, mock_dataset, mock_datasets
):
    dataset_metadata = SDTMDatasetMetadata(name="LB", first_record={"DOMAIN": "LB"})
    variables = get_variables_metadata_from_standard(
        "LB",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/lb.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "LBTEST" for var in variables)
    assert any(var["name"] == "LBORRES" for var in variables)


def test_supp_domain(library_metadata, mock_data_service, mock_dataset, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="SUPPAE", first_record={"QNAM": "TEST"})
    variables = get_variables_metadata_from_standard(
        "SUPPAE",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/suppae.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "QNAM" for var in variables)
    assert any(var["name"] == "QLABEL" for var in variables)


def test_sq_domain(library_metadata, mock_data_service, mock_dataset, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="SQAE", first_record={"QNAM": "TEST"})
    variables = get_variables_metadata_from_standard(
        "SQAE",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/sqae.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "QNAM" for var in variables)
    assert any(var["name"] == "QLABEL" for var in variables)


def test_ap_domain(library_metadata, mock_data_service, mock_dataset, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="APDM", first_record={"APID": "001"})
    variables = get_variables_metadata_from_standard(
        "APDM",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/apdm.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "APID" for var in variables)
    assert not any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "RSUBJID" for var in variables)
    assert any(var["name"] == "RACE" for var in variables)
    assert any(var["name"] == "DMDY" for var in variables)


def test_sqap_domain(library_metadata, mock_data_service, mock_dataset, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="SQAP", first_record={"QNAM": "TEST"})
    variables = get_variables_metadata_from_standard(
        "SQAP",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/sqap.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "APID" for var in variables)
    assert not any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "RDOMAIN" for var in variables)


def test_findings_about_domain_fa(
    library_metadata, mock_data_service, mock_dataset, mock_datasets
):
    """Test Findings About domain includes FINDINGS class variables."""
    dataset_metadata = SDTMDatasetMetadata(name="FA", first_record={"DOMAIN": "FA"})
    variables = get_variables_metadata_from_standard(
        "FA",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/fa.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "FATEST" for var in variables)
    assert any(var["name"] == "FAOBJ" for var in variables)


# Tests for get_variables_metadata_from_standard_model
def test_findings_domain_from_model(library_metadata, mock_data_service, mock_datasets):
    mock_dataframe = Mock()
    dataset_metadata = SDTMDatasetMetadata(name="LB", first_record={"DOMAIN": "LB"})
    variables = get_variables_metadata_from_standard_model(
        domain="LB",
        dataframe=mock_dataframe,
        datasets=mock_datasets,
        dataset_path="/path/to/lb.xpt",
        data_service=mock_data_service,
        library_metadata=library_metadata,
        dataset_metadata=dataset_metadata,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "LBTEST" for var in variables)


def test_supp_domain_from_model(library_metadata, mock_data_service, mock_datasets):
    """Test retrieving variables for SUPP domain from model."""
    mock_dataframe = Mock()
    dataset_metadata = SDTMDatasetMetadata(name="SUPPAE", first_record={"QNAM": "TEST"})
    variables = get_variables_metadata_from_standard_model(
        domain="SUPPAE",
        dataframe=mock_dataframe,
        datasets=mock_datasets,
        dataset_path="/path/to/suppae.xpt",
        data_service=mock_data_service,
        library_metadata=library_metadata,
        dataset_metadata=dataset_metadata,
    )
    assert any(var["name"] == "RDOMAIN" for var in variables)
    assert any(var["name"] == "IDVAR" for var in variables)


def test_sqap_domain_from_model(library_metadata, mock_data_service, mock_datasets):
    """Test retrieving variables for SUPP domain from model."""
    mock_dataframe = Mock()
    dataset_metadata = SDTMDatasetMetadata(name="SQAP", first_record={"QNAM": "TEST"})
    variables = get_variables_metadata_from_standard_model(
        domain="SQAP",
        dataframe=mock_dataframe,
        datasets=mock_datasets,
        dataset_path="/path/to/suppae.xpt",
        data_service=mock_data_service,
        library_metadata=library_metadata,
        dataset_metadata=dataset_metadata,
    )
    assert any(var["name"] == "RDOMAIN" for var in variables)
    assert any(var["name"] == "APID" for var in variables)


def test_ap_domain_from_model(library_metadata, mock_data_service, mock_datasets):
    """Test AP domain excludes USUBJID and includes APID."""
    mock_dataframe = Mock()
    dataset_metadata = SDTMDatasetMetadata(name="APDM", first_record={"APID": "001"})
    variables = get_variables_metadata_from_standard_model(
        domain="APDM",
        dataframe=mock_dataframe,
        datasets=mock_datasets,
        dataset_path="/path/to/apdm.xpt",
        data_service=mock_data_service,
        library_metadata=library_metadata,
        dataset_metadata=dataset_metadata,
    )
    assert not any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "APID" for var in variables)
    assert any(var["name"] == "AGE" for var in variables)
    assert any(var["name"] == "DMDY" for var in variables)


def test_custom_domain_events_class(
    library_metadata, mock_data_service, mock_dataset, mock_datasets
):
    """Test custom domain detection and variable metadata retrieval for EVENTS class."""
    dataset_metadata = SDTMDatasetMetadata(name="ZZ", first_record={"DOMAIN": "ZZ"})
    mock_data_service._handle_custom_domains = Mock(return_value="EVENTS")
    mock_dataset.columns = ["STUDYID", "DOMAIN", "USUBJID", "ZZTERM", "ZZSEQ"]
    variables = get_variables_metadata_from_standard(
        "ZZ",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/zz.xpt",
        mock_datasets,
    )
    mock_data_service._handle_custom_domains.assert_called_once()
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "DOMAIN" for var in variables)
    assert any(var["name"] == "ZZTERM" for var in variables)
    assert any(var["name"] == "ZZSEQ" for var in variables)


def test_custom_domain_findings_class(
    library_metadata, mock_data_service, mock_dataset, mock_datasets
):
    """Test custom domain detection and variable metadata retrieval for FINDINGS class."""
    dataset_metadata = SDTMDatasetMetadata(name="XX", first_record={"DOMAIN": "XX"})
    mock_data_service._handle_custom_domains = Mock(return_value="FINDINGS")
    mock_dataset.columns = ["STUDYID", "DOMAIN", "USUBJID", "XXTESTCD", "XXORRES"]
    variables = get_variables_metadata_from_standard(
        "XX",
        library_metadata,
        mock_data_service,
        mock_dataset,
        dataset_metadata,
        "/path/to/xx.xpt",
        mock_datasets,
    )
    mock_data_service._handle_custom_domains.assert_called_once()
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "DOMAIN" for var in variables)
    assert any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "XXTESTCD" for var in variables)
    assert any(var["name"] == "XXORRES" for var in variables)
