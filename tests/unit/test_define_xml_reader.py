"""
This module contains unit tests for DefineXMLReader class.
"""
import os
from typing import List

import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import (
    DomainNotFoundInDefineXMLError,
)
from cdisc_rules_engine.services.define_xml_reader import DefineXMLReader

test_define_file_path: str = (
    f"{os.path.dirname(__file__)}/../resources/test_defineV22-SDTM.xml"
)


def test_init_from_filename():
    """
    Unit test for DefineXMLReader.from_filename constructor.
    """
    reader = DefineXMLReader.from_filename(test_define_file_path)
    assert isinstance(reader, DefineXMLReader)


def test_init_from_file_contents():
    """
    Unit test for DefineXMLReader.from_file_contents constructor.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReader.from_file_contents(contents)
        assert isinstance(reader, DefineXMLReader)


def test_read_define_xml():
    """
    Unit test for DefineXMLReader.read function.
    The test uses from_file_contents constructor because
    it is the most common use case for the class.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReader.from_file_contents(contents)
        metadata: List[dict] = reader.read()
        for item in metadata:
            assert isinstance(item, dict)
            assert list(item.keys()) == [
                "dataset_name",
                "dataset_label",
                "dataset_location",
                "dataset_class",
                "dataset_structure",
                "dataset_is_non_standard",
            ]


def test_extract_domain_metadata():
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReader.from_file_contents(contents)
        domain_metadata: dict = reader.extract_domain_metadata(domain_name="TS")
        assert domain_metadata == {
            "dataset_name": "TS",
            "dataset_label": "Trial Summary",
            "dataset_location": "ts.xml",
            "dataset_class": "TRIAL DESIGN",
            "dataset_structure": "One record per trial summary parameter value",
            "dataset_is_non_standard": "None",
        }


def test_extract_variable_metadata():
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReader.from_file_contents(contents)
        variable_metadata: List[dict] = reader.extract_variables_metadata(
            domain_name="EX"
        )
        expected_variables = {
            "EXSEQ",
            "EXTRT",
            "EXDOSE",
            "EXDOSU",
            "EXDOSFRM",
            "EXSTDTC",
            "EXENDTC",
            "EXSTDY",
            "EXENDY",
            "STUDYID",
            "DOMAIN",
            "USUBJID",
            "SPDEVID",
            "EXDOSFRQ",
            "EXROUTE",
            "EXLOT",
            "EPOCH",
        }
        expected_exroute_metadata = {
            "define_variable_name": "EXROUTE",
            "define_variable_label": "Route of Administration",
            "define_variable_data_type": "Char",
            "define_variable_size": 12,
            "define_variable_role": "Variable Qualifier",
            "define_variable_ccode": "C66729",
            "define_variable_allowed_terms": ["Subcutaneous Route of Administration"],
            "define_variable_origin_type": "Predecessor",
        }
        for variable in variable_metadata:
            assert variable["define_variable_name"] in expected_variables
            if (
                variable["define_variable_name"]
                == expected_exroute_metadata["define_variable_name"]
            ):
                for key in expected_exroute_metadata.keys():
                    assert variable[key] == expected_exroute_metadata[key]


def test_extract_value_level_metadata():
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReader.from_file_contents(contents)
        value_level_metadata: dict = reader.extract_value_level_metadata(
            domain_name="AE"
        )
        assert len(value_level_metadata) == 2
        for vlm in value_level_metadata:
            assert vlm["define_variable_name"] == "AETERM"
        mock_invalid_length_row_data = {"AETERM": "A" * 201}
        mock_invalid_type_row_data = {"AETERM": 201}
        mock_valid_row_data = {"AETERM": "A" * 200}
        mock_filter_pass_row_data = {"AETERM": "INJECTION SITE REACTION"}
        mock_filter_fail_row_data = {"AETERM": "ALL_GOOD"}
        assert (
            value_level_metadata[0]["type_check"](mock_invalid_type_row_data) == False
        )
        assert (
            value_level_metadata[0]["length_check"](mock_invalid_length_row_data)
            == False
        )
        assert value_level_metadata[0]["type_check"](mock_valid_row_data) == True
        assert value_level_metadata[0]["length_check"](mock_valid_row_data) == True
        assert value_level_metadata[0]["filter"](mock_filter_pass_row_data) == True
        assert value_level_metadata[0]["filter"](mock_filter_fail_row_data) == False


def test_extract_domain_metadata_not_found():
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    Test the case when requested domain is not found in Define XML.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReader.from_file_contents(contents)
        with pytest.raises(DomainNotFoundInDefineXMLError):
            reader.extract_domain_metadata(domain_name="not found domain")


def test_validate_schema():
    """
    Unit test for DefineXMLReader.validate_schema function.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReader.from_file_contents(contents)
        assert reader.validate_schema() is True


def test_extract_variable_metadata_not_found():
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    Test the case when requested domain is not found in Define XML.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReader.from_file_contents(contents)
        with pytest.raises(DomainNotFoundInDefineXMLError):
            reader.extract_variables_metadata(domain_name="not found domain")
