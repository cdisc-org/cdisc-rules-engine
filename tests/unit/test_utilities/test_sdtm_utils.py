import pytest
from unittest.mock import Mock, patch
from scripts.script_utils import (
    get_library_metadata_from_cache,
)

from cdisc_rules_engine.utilities.sdtm_utilities import (
    get_corresponding_datasets,
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


def test_standard_domain_ae(library_metadata, mock_data_service, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="AE", first_record={"DOMAIN": "AE"})
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
        dataset_metadata,
        "/path/to/ae.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "AETERM" for var in variables)
    assert any(var["name"] == "AESTDTC" for var in variables)


def test_standard_domain_dm(library_metadata, mock_data_service, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="DM", first_record={"DOMAIN": "DM"})
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
        dataset_metadata,
        "/path/to/dm.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "AGE" for var in variables)
    assert any(var["name"] == "SEX" for var in variables)


def test_findings_domain_lb(library_metadata, mock_data_service, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="LB", first_record={"DOMAIN": "LB"})
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
        dataset_metadata,
        "/path/to/lb.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "LBTEST" for var in variables)
    assert any(var["name"] == "LBORRES" for var in variables)


def test_supp_domain(library_metadata, mock_data_service, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="SUPPAE", first_record={"QNAM": "TEST"})
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
        dataset_metadata,
        "/path/to/suppae.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "QNAM" for var in variables)
    assert any(var["name"] == "QLABEL" for var in variables)


def test_sq_domain(library_metadata, mock_data_service, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="SQAE", first_record={"QNAM": "TEST"})
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
        dataset_metadata,
        "/path/to/sqae.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "STUDYID" for var in variables)
    assert any(var["name"] == "QNAM" for var in variables)
    assert any(var["name"] == "QLABEL" for var in variables)


def test_ap_domain(library_metadata, mock_data_service, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(name="APDM", first_record={"APID": "001"})
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
        dataset_metadata,
        "/path/to/apdm.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "APID" for var in variables)
    assert not any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "RSUBJID" for var in variables)
    assert any(var["name"] == "RACE" for var in variables)
    assert any(var["name"] == "DMDY" for var in variables)


def test_sqap_domain(library_metadata, mock_data_service, mock_datasets):
    dataset_metadata = SDTMDatasetMetadata(
        name="SQAPMH", first_record={"QNAM": "TEST", "RDOMAIN": "APMH"}
    )
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
        dataset_metadata,
        "/path/to/sqapmh.xpt",
        mock_datasets,
    )
    assert any(var["name"] == "APID" for var in variables)
    assert not any(var["name"] == "USUBJID" for var in variables)
    assert any(var["name"] == "RDOMAIN" for var in variables)


def test_findings_about_domain_fa(library_metadata, mock_data_service, mock_datasets):
    """Test Findings About domain includes FINDINGS class variables."""
    dataset_metadata = SDTMDatasetMetadata(name="FA", first_record={"DOMAIN": "FA"})
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
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


def test_custom_domain_events_class(library_metadata, mock_data_service, mock_datasets):
    """Test custom domain detection and variable metadata retrieval for EVENTS class."""
    dataset_metadata = SDTMDatasetMetadata(name="ZZ", first_record={"DOMAIN": "ZZ"})
    mock_data_service._handle_custom_domains = Mock(return_value="EVENTS")
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
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
    library_metadata, mock_data_service, mock_datasets
):
    """Test custom domain detection and variable metadata retrieval for FINDINGS class."""
    dataset_metadata = SDTMDatasetMetadata(name="XX", first_record={"DOMAIN": "XX"})
    mock_data_service._handle_custom_domains = Mock(return_value="FINDINGS")
    variables = get_variables_metadata_from_standard(
        library_metadata,
        mock_data_service,
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


mock_datasets_ss = [
    {"filename": "SS11.xpt", "first_record": {"DOMAIN": "SS"}},
]


@patch(
    "cdisc_rules_engine.utilities.sdtm_utilities.get_corresponding_datasets",
    return_value=mock_datasets_ss,
)
def test_is_split_dataset_from_file(mock_get_corresponding_datasets):
    result = (
        SDTMDatasetMetadata(
            filename="SS11.xpt", first_record={"DOMAIN": "SS"}
        ).is_split,
    )
    assert result


datasets_tests = [
    (
        {"name": "SS", "first_record": {"RDOMAIN": "SS"}},
        False,
    ),
    (
        {"name": "SUPPSS", "first_record": {"RDOMAIN": "SS"}},
        True,
    ),
    ({"name": "SUPPSS1", "first_record": {"RDOMAIN": "SS"}}, True),
    ({"name": "SQAPSSS1", "first_record": {"RDOMAIN": "APSS"}}, True),
]


@pytest.mark.parametrize("mock_dataset, expected", datasets_tests)
def test_is_supp_dataset(mock_dataset, expected):
    result = SDTMDatasetMetadata(**mock_dataset).is_supp
    assert (
        result == expected
    ), f"Expected {expected} but got {result} for datasets {datasets_tests}"


is_ap_tests = [
    ({"first_record": {"DOMAIN": "APFA", "APID": "AP001"}}, True),
    ({"first_record": {"DOMAIN": "APXX", "APID": "AP002"}}, True),
    ({"first_record": {"DOMAIN": "APQS", "APID": "AP003"}}, True),
    ({"first_record": {"DOMAIN": "APFAMH", "APID": "AP004"}}, True),
    ({"first_record": {"DOMAIN": "AE"}}, False),
    ({"first_record": {"DOMAIN": "LB"}}, False),
    ({"first_record": {"DOMAIN": "AP"}}, False),
    ({"first_record": {"DOMAIN": "APF"}}, False),
    ({"first_record": None}, False),
    ({"first_record": {}}, False),
    ({}, False),
    ({"name": "SQAPQS", "first_record": {"RDOMAIN": "APQS"}}, True),
    ({"name": "SQAPQSX", "first_record": {"RDOMAIN": "APQS"}}, True),
    ({"name": "SQAPQSXX", "first_record": {"RDOMAIN": "APQS"}}, True),
    ({"name": "SUPPQS", "first_record": {"RDOMAIN": "QS"}}, False),
    ({"name": "SQAPQS", "first_record": {"RDOMAIN": "AP"}}, False),
    ({"name": "SQAPQS", "first_record": {"RDOMAIN": "APF"}}, False),
    ({"first_record": {"APID": "AP001"}}, True),
    ({"first_record": {"DOMAIN": "AP", "APID": "AP001"}}, True),
    ({"first_record": {"DOMAIN": "APF", "APID": "AP001"}}, True),
]


@pytest.mark.parametrize("mock_dataset, expected", is_ap_tests)
def test_is_ap_dataset(mock_dataset, expected):
    result = SDTMDatasetMetadata(**mock_dataset).is_ap
    assert (
        result == expected
    ), f"Expected {expected} but got {result} for dataset {mock_dataset}"


ap_suffix_tests = [
    ({"first_record": {"DOMAIN": "APFA", "APID": "AP001"}}, "FA"),
    ({"first_record": {"DOMAIN": "APXX", "APID": "AP002"}}, "XX"),
    ({"first_record": {"DOMAIN": "APQS", "APID": "AP003"}}, "QS"),
    ({"first_record": {"DOMAIN": "APLB", "APID": "AP004"}}, "LB"),
    ({"first_record": {"DOMAIN": "APFA", "APID": "AP005"}}, "FA"),
    ({"first_record": {"DOMAIN": "AE"}}, ""),
    ({"first_record": {"DOMAIN": "LB"}}, ""),
    ({"first_record": {"DOMAIN": "AP"}}, ""),
    ({"first_record": {"DOMAIN": "APF"}}, ""),
    ({"first_record": None}, ""),
    ({"first_record": {}}, ""),
    ({}, ""),
    ({"name": "SQAPQS", "first_record": {"RDOMAIN": "APQS"}}, ""),
    ({"name": "SQAPQSX", "first_record": {"RDOMAIN": "APQS"}}, ""),
    ({"name": "SQAPQSXX", "first_record": {"RDOMAIN": "APQS"}}, ""),
    ({"first_record": {"APID": "AP001"}}, ""),
    ({"first_record": {"DOMAIN": "AP", "APID": "AP001"}}, ""),
    ({"first_record": {"DOMAIN": "APF", "APID": "AP001"}}, ""),
]


@pytest.mark.parametrize("mock_dataset, expected", ap_suffix_tests)
def test_ap_suffix_property(mock_dataset, expected):
    result = SDTMDatasetMetadata(**mock_dataset).ap_suffix
    assert (
        result == expected
    ), f"Expected {expected} but got {result} for dataset {mock_dataset}"


datasets = [
    SDTMDatasetMetadata(**dataset)
    for dataset in [
        {"filename": "SS.xpt", "first_record": {"DOMAIN": "SS"}},
        {"filename": "SS12.xpt", "first_record": {"DOMAIN": "SS"}},
        {"filename": "AE.xpt", "first_record": {"DOMAIN": "AE"}},
        {"filename": "DD.xpt", "first_record": {"DOMAIN": "DD"}},
        {"filename": "EC.xpt", "first_record": {"DOMAIN": "EC"}},
        {"filename": "EX.xpt", "first_record": {"DOMAIN": "EX"}},
        {"filename": "FA.xpt", "first_record": {"DOMAIN": "FA"}},
        {"filename": "FT.xpt", "first_record": {"DOMAIN": "FT"}},
        {"filename": "RS.xpt", "first_record": {"DOMAIN": "RS"}},
        {"filename": "AB.xpt", "first_record": {"DOMAIN": "AB"}},
        {"filename": "AB12.xpt", "first_record": {"DOMAIN": "AB"}},
    ]
]


# Parameters for testing each domain
domain_test_cases = [
    (
        "SS",
        [
            {"filename": "SS.xpt", "first_record": {"DOMAIN": "SS"}},
            {"filename": "SS12.xpt", "first_record": {"DOMAIN": "SS"}},
        ],
    ),
    (
        "AB",
        [
            {"filename": "AB.xpt", "first_record": {"DOMAIN": "AB"}},
            {"filename": "AB12.xpt", "first_record": {"DOMAIN": "AB"}},
        ],
    ),
    ("AE", [{"filename": "AE.xpt", "first_record": {"DOMAIN": "AE"}}]),
    ("DD", [{"filename": "DD.xpt", "first_record": {"DOMAIN": "DD"}}]),
    ("EC", [{"filename": "EC.xpt", "first_record": {"DOMAIN": "EC"}}]),
    ("EX", [{"filename": "EX.xpt", "first_record": {"DOMAIN": "EX"}}]),
    ("FA", [{"filename": "FA.xpt", "first_record": {"DOMAIN": "FA"}}]),
    ("FT", [{"filename": "FT.xpt", "first_record": {"DOMAIN": "FT"}}]),
    ("RS", [{"filename": "RS.xpt", "first_record": {"DOMAIN": "RS"}}]),
]


@pytest.mark.parametrize("domain, expected_datasets", domain_test_cases)
def test_get_corresponding_datasets(domain, expected_datasets):
    result_datasets = get_corresponding_datasets(
        datasets, SDTMDatasetMetadata(first_record={"DOMAIN": domain})
    )
    assert result_datasets == [
        SDTMDatasetMetadata(**dataset) for dataset in expected_datasets
    ], f"The function should return only datasets matching the '{domain}' domain"
