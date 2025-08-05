import pytest
from pathlib import Path
from cdisc_rules_engine.readers.define_xml_reader import XMLReader, DefineXMLMetadata


@pytest.fixture
def define_xml_directory(resources_directory: Path) -> Path:
    """
    Returns the path to a test Define-XML file.
    """
    return resources_directory / "define_xml"


def get_all_xml_files(define_xml_directory) -> list[Path]:
    """Get all xml files from the directory."""
    files = []
    files.extend(define_xml_directory.glob(".xml"))
    return sorted(files)


def test_xml_reader_init(define_xml_directory):
    """
    Tests that the XMLReader can be initialised with a valid file path.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        assert reader.file_path == define_xml_file
        assert isinstance(reader.metadata, DefineXMLMetadata)


def test_extract_metadata(define_xml_directory):
    """
    Tests that metadata is correctly extracted from the Define-XML file.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        metadata = reader.metadata
        assert metadata.file_type == "define-xml"
        assert metadata.odm_version == "1.3.2"
        assert metadata.file_oid is not None
        assert metadata.creation_datetime is not None


def test_read_define_xml(define_xml_directory):
    """
    Tests that the read method returns a dictionary of lists of dictionaries.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        data = reader.read()
        assert isinstance(data, dict)
        for table_name, table_data in data.items():
            assert isinstance(table_data, list)
            if table_data:
                assert isinstance(table_data[0], dict)


def test_studies_extraction(define_xml_directory):
    """
    Tests that the studies table is extracted correctly.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        data = reader.read()
        assert "studies" in data
        assert len(data["studies"]) > 0
        study = data["studies"]
        assert "study_id" in study
        assert "study_oid" in study
        assert "study_name" in study
        assert "study_description" in study
        assert "protocol_name" in study


def test_metadata_versions_extraction(define_xml_directory):
    """
    Tests that the metadata_versions table is extracted correctly.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        data = reader.read()
        assert "metadata_versions" in data
        assert len(data["metadata_versions"]) > 0
        metadata_version = data["metadata_versions"]
        assert "version_id" in metadata_version
        assert "study_id" in metadata_version
        assert "version_oid" in metadata_version
        assert "define_version" in metadata_version


def test_datasets_extraction(define_xml_directory):
    """
    Tests that the datasets table is extracted correctly.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        data = reader.read()
        assert "datasets" in data
        assert len(data["datasets"]) > 0
        dataset = data["datasets"][0]
        assert "dataset_id" in dataset
        assert "version_id" in dataset
        assert "dataset_oid" in dataset
        assert "dataset_name" in dataset
        assert "structure" in dataset
        assert "class" in dataset


def test_variables_extraction(define_xml_directory):
    """
    Tests that the variables table is extracted correctly.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        data = reader.read()
        assert "variables" in data
        assert len(data["variables"]) > 0
        variable = data["variables"][0]
        assert "variable_id" in variable
        assert "version_id" in variable
        assert "variable_oid" in variable
        assert "variable_name" in variable
        assert "data_type" in variable


def test_codelists_extraction(define_xml_directory):
    """
    Tests that the codelists and codelist_items tables are extracted correctly.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        data = reader.read()
        assert "codelists" in data
        assert "codelist_items" in data
        assert len(data["codelists"]) > 0
        assert len(data["codelist_items"]) > 0
        codelist = data["codelists"][0]
        assert "codelist_id" in codelist
        assert "version_id" in codelist
        assert "codelist_oid" in codelist
        codelist_item = data["codelist_items"][0]
        assert "item_id" in codelist_item
        assert "codelist_id" in codelist_item
        assert "coded_value" in codelist_item


def test_methods_extraction(define_xml_directory):
    """
    Tests that the methods table is extracted correctly.
    """
    for define_xml_file in get_all_xml_files(define_xml_directory):
        reader = XMLReader(str(define_xml_file))
        data = reader.read()
        assert "methods" in data
        assert len(data["methods"]) > 0
        method = data["methods"][0]
        assert "method_id" in method
        assert "version_id" in method
        assert "method_oid" in method
        assert "method_type" in method
