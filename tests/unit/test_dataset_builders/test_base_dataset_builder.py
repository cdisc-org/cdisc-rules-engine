import pytest
from types import MethodType
from unittest.mock import MagicMock, patch
from cdisc_rules_engine.dataset_builders.base_dataset_builder import (
    BaseDatasetBuilder,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.services.data_services import LocalDataService
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
)
from cdisc_rules_engine.exceptions.custom_exceptions import (
    DomainNotFoundInDefineXMLError,
)


class ConcreteDatasetBuilder(BaseDatasetBuilder):
    """Concrete implementation for testing BaseDatasetBuilder"""

    def build(self):
        """Concrete implementation of abstract build method"""
        pass


def create_mock_reader_with_metadata(item_group_defs, item_defs=None):
    """
    Helper function to create a mock reader with metadata structure.

    Args:
        item_group_defs: List of mock ItemGroupDef objects
        item_defs: List of mock ItemDef objects (optional)

    Returns:
        Mock reader with configured metadata
    """
    # Mock MetaDataVersion with all required attributes
    mock_metadata = MagicMock()
    mock_metadata.ItemGroupDef = item_group_defs
    mock_metadata.ItemDef = item_defs if item_defs is not None else []
    mock_metadata.CodeList = []

    # Mock the reader
    mock_reader = MagicMock()
    mock_reader._odm_loader.MetaDataVersion.return_value = mock_metadata
    # Set cache_service to None so the @cached decorator doesn't interfere
    mock_reader.cache_service = None
    mock_reader.study_id = None
    mock_reader.data_bundle_id = None

    # Bind the actual methods from BaseDefineXMLReader to the mock reader
    mock_reader._get_domain_metadata = MethodType(
        BaseDefineXMLReader._get_domain_metadata, mock_reader
    )
    mock_reader.extract_variables_metadata = MethodType(
        BaseDefineXMLReader.extract_variables_metadata, mock_reader
    )

    return mock_reader


def create_builder_instance(dataset_metadata, dataset_path="/path/to/dataset.xpt"):
    """
    Helper function to create a ConcreteDatasetBuilder instance.

    Args:
        dataset_metadata: SDTMDatasetMetadata instance
        dataset_path: Path to the dataset file

    Returns:
        ConcreteDatasetBuilder instance
    """
    return ConcreteDatasetBuilder(
        rule=MagicMock(),
        data_service=LocalDataService(MagicMock(), MagicMock(), MagicMock()),
        cache_service=MagicMock(),
        rule_processor=MagicMock(),
        data_processor=MagicMock(),
        dataset_path=dataset_path,
        datasets=[dataset_metadata],
        dataset_metadata=dataset_metadata,
        define_xml_path="/path/to/define.xml",
        standard="sdtmig",
        standard_version="3-4",
        standard_substandard=None,
        library_metadata=LibraryMetadataContainer(),
    )


@pytest.mark.parametrize(
    "dataset_name, first_record, expected_domain",
    [
        ("AE", {"DOMAIN": "AE"}, "AE"),  # Regular dataset with domain
        ("SUPPAE", {"RDOMAIN": "AE"}, "AE"),  # Supplemental dataset with rdomain
        ("QSPH", {"DOMAIN": "QS"}, "QS"),  # Split dataset with domain
        ("RELREC", {}, None),  # RELREC with no domain
    ],
)
@patch(
    "cdisc_rules_engine.dataset_builders.base_dataset_builder."
    "DefineXMLReaderFactory.get_define_xml_reader"
)
def test_get_define_xml_variables_metadata(
    mock_get_define_xml_reader: MagicMock,
    dataset_name: str,
    first_record: dict,
    expected_domain: str,
):
    """
    Test that get_define_xml_variables_metadata correctly:
    1. Gets the DefineXMLReader using the factory
    2. Mocks _odm_loader.MetaDataVersion() to return proper metadata structure
    3. Allows _get_domain_metadata to work naturally with the mocked structure
    4. Verifies the correct domain/name parameters are used
    """
    # Mock ItemDef objects
    mock_item_def_1 = MagicMock()
    mock_item_def_1.OID = "IT.STUDYID"
    mock_item_def_1.Name = "STUDYID"

    mock_item_def_2 = MagicMock()
    mock_item_def_2.OID = "IT.USUBJID"
    mock_item_def_2.Name = "USUBJID"

    # Mock ItemRef objects
    mock_item_ref_1 = MagicMock()
    mock_item_ref_1.ItemOID = "IT.STUDYID"

    mock_item_ref_2 = MagicMock()
    mock_item_ref_2.ItemOID = "IT.USUBJID"

    # Mock ItemGroupDef (domain metadata)
    mock_item_group_def = MagicMock()
    mock_item_group_def.Name = dataset_name
    mock_item_group_def.Domain = expected_domain
    mock_item_group_def.ItemRef = [mock_item_ref_1, mock_item_ref_2]

    # Create mock reader with metadata
    mock_reader = create_mock_reader_with_metadata(
        item_group_defs=[mock_item_group_def],
        item_defs=[mock_item_def_1, mock_item_def_2],
    )

    # Expected output from _get_item_def_representation
    expected_variables_metadata = [
        {
            "define_variable_name": "STUDYID",
            "define_variable_label": "Study Identifier",
            "define_variable_data_type": "text",
            "define_variable_role": "Identifier",
        },
        {
            "define_variable_name": "USUBJID",
            "define_variable_label": "Unique Subject Identifier",
            "define_variable_data_type": "text",
            "define_variable_role": "Identifier",
        },
    ]

    mock_reader._get_item_def_representation = MagicMock(
        side_effect=expected_variables_metadata
    )
    mock_reader._get_codelist_def_map = MagicMock(return_value={})
    mock_get_define_xml_reader.return_value = mock_reader

    # Setup dataset metadata
    dataset_metadata = SDTMDatasetMetadata(
        name=dataset_name,
        filename=f"{dataset_name.lower()}.xpt",
        label=f"{dataset_name} Label",
        first_record=first_record,
    )

    # Create builder instance
    builder = create_builder_instance(dataset_metadata, "/path/to/dataset.xpt")

    # Call the method
    result = builder.get_define_xml_variables_metadata()

    # Verify DefineXMLReaderFactory was called correctly
    mock_get_define_xml_reader.assert_called_once_with(
        "/path/to/dataset.xpt",
        "/path/to/define.xml",
        builder.data_service,
        builder.cache,
    )

    # Verify the result is returned correctly
    assert result == expected_variables_metadata


@patch(
    "cdisc_rules_engine.dataset_builders.base_dataset_builder."
    "DefineXMLReaderFactory.get_define_xml_reader"
)
def test_get_define_xml_variables_metadata_domain_not_found(
    mock_get_define_xml_reader: MagicMock,
):
    """
    Test that get_define_xml_variables_metadata raises DomainNotFoundInDefineXMLError
    when the domain/name combination is not found in the Define XML metadata.
    """
    # Mock ItemGroupDef for a different dataset (DM) than what we'll request (AE)
    mock_item_group_def = MagicMock()
    mock_item_group_def.Name = "DM"
    mock_item_group_def.Domain = "DM"
    mock_item_group_def.ItemRef = []

    # Create mock reader with metadata that doesn't include AE
    mock_reader = create_mock_reader_with_metadata(
        item_group_defs=[mock_item_group_def],
        item_defs=[],
    )
    mock_get_define_xml_reader.return_value = mock_reader

    # Setup dataset metadata for AE (which doesn't exist in metadata)
    dataset_metadata = SDTMDatasetMetadata(
        name="AE",
        filename="ae.xpt",
        label="Adverse Events",
        first_record={"DOMAIN": "AE"},
    )

    # Create builder instance
    builder = create_builder_instance(dataset_metadata, "/path/to/ae.xpt")

    # Verify that DomainNotFoundInDefineXMLError is raised
    with pytest.raises(
        DomainNotFoundInDefineXMLError,
        match=r"name=AE, domain=AE is not found in Define XML",
    ):
        builder.get_define_xml_variables_metadata()
