"""
This module contains unit tests for DefineXMLReader class.
"""
from pathlib import Path
from typing import List

import pytest

from cdisc_rules_engine.exceptions.custom_exceptions import (
    DomainNotFoundInDefineXMLError,
)
from cdisc_rules_engine.services.define_xml.base_define_xml_reader import (
    BaseDefineXMLReader,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_2_1 import (
    DefineXMLReader21,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_2_0 import (
    DefineXMLReader20,
)

resources_path: Path = Path(__file__).parent.parent.joinpath("resources")
test_define_2_0_file_path: Path = resources_path.joinpath("test_defineV20-SDTM.xml")
test_define_file_path: Path = resources_path.joinpath("test_defineV22-SDTM.xml")


def test_init_from_filename_v2_1():
    """
    Unit test for DefineXMLReader.from_filename constructor.
    """
    reader = DefineXMLReaderFactory.from_filename(test_define_file_path)
    assert isinstance(reader, DefineXMLReader21)


def test_init_from_filename_v2_0():
    """
    Unit test for DefineXMLReader.from_filename constructor.
    """
    reader = DefineXMLReaderFactory.from_filename(test_define_2_0_file_path)
    assert isinstance(reader, DefineXMLReader20)


def test_init_from_file_contents():
    """
    Unit test for DefineXMLReader.from_file_contents constructor.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        assert isinstance(reader, BaseDefineXMLReader)


def test_read_define_xml():
    """
    Unit test for DefineXMLReader.read function.
    The test uses from_file_contents constructor because
    it is the most common use case for the class.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        metadata: List[dict] = reader.read()
        for item in metadata:
            assert isinstance(item, dict)
            assert list(item.keys()) == [
                "define_dataset_name",
                "define_dataset_label",
                "define_dataset_location",
                "define_dataset_class",
                "define_dataset_structure",
                "define_dataset_is_non_standard",
            ]


@pytest.mark.parametrize(
    "filename", [(test_define_file_path), (test_define_2_0_file_path)]
)
def test_extract_domain_metadata(filename):
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    """
    with open(filename, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        domain_metadata: dict = reader.extract_domain_metadata(domain_name="TS")
        assert domain_metadata == {
            "define_dataset_name": "TS",
            "define_dataset_label": "Trial Summary",
            "define_dataset_location": "ts.xml",
            "define_dataset_class": "TRIAL DESIGN",
            "define_dataset_structure": "One record per trial summary parameter value",
            "define_dataset_is_non_standard": "",
            "define_dataset_variables": [
                "STUDYID",
                "DOMAIN",
                "TSSEQ",
                "TSGRPID",
                "TSPARMCD",
                "TSPARM",
                "TSVAL",
                "TSVALNF",
                "TSVALCD",
                "TSVCDREF",
                "TSVCDVER",
            ],
        }


@pytest.mark.parametrize(
    "filename", [(test_define_file_path), (test_define_2_0_file_path)]
)
def test_extract_variable_metadata(filename):
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    """
    with open(filename, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
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
            "define_variable_data_type": "text",
            "define_variable_size": 12,
            "define_variable_role": "Variable Qualifier",
            "define_variable_ccode": "C66729",
            "define_variable_allowed_terms": ["Subcutaneous Route of Administration"],
            "define_variable_origin_type": "Predecessor",
            "define_variable_is_collected": False,
            "define_variable_order_number": 11,
            "define_variable_has_comment": True,
        }
        for variable in variable_metadata:
            assert variable["define_variable_name"] in expected_variables
            if (
                variable["define_variable_name"]
                == expected_exroute_metadata["define_variable_name"]
            ):
                for key in expected_exroute_metadata.keys():
                    assert variable[key] == expected_exroute_metadata[key]


def test_extract_variable_metadata_with_has_no_data():
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        variable_metadata: List[dict] = reader.extract_variables_metadata(
            domain_name="AE"
        )
        target_variable = next(
            iter(
                [
                    variable
                    for variable in variable_metadata
                    if variable["define_variable_name"] == "AEHLT"
                ]
            )
        )
        assert target_variable["define_variable_has_no_data"] == "Yes"


@pytest.mark.parametrize(
    "define_file_path, domain_name, define_variable_name, expected",
    [
        (
            test_define_2_0_file_path,
            "AE",
            "AEACN",
            True,
        ),
        (
            test_define_2_0_file_path,
            "AE",
            "AEBODSYS",
            False,
        ),
        (
            test_define_2_0_file_path,
            "LB",
            "LBCAT",
            False,
        ),
        (
            test_define_file_path,
            "AE",
            "AEACN",
            True,
        ),
        (
            test_define_file_path,
            "AE",
            "AEBODSYS",
            False,
        ),
        (
            test_define_file_path,
            "LB",
            "LBSEQ",
            False,
        ),
    ],
)
def test_define_variable_is_collected(
    define_file_path, domain_name, define_variable_name, expected
):
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    """
    with open(define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        variable_metadata: List[dict] = reader.extract_variables_metadata(
            domain_name=domain_name
        )
        variable = [
            variable
            for variable in variable_metadata
            if variable["define_variable_name"] == define_variable_name
        ]
        assert len(variable) == 1
        assert variable[0]["define_variable_is_collected"] == expected


@pytest.mark.parametrize(
    "filename", [(test_define_file_path), (test_define_2_0_file_path)]
)
def test_extract_value_level_metadata(filename):
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    """
    with open(filename, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
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
            value_level_metadata[0]["type_check"](mock_invalid_type_row_data) is False
        )
        assert (
            value_level_metadata[0]["length_check"](mock_invalid_length_row_data)
            is False
        )
        assert value_level_metadata[0]["type_check"](mock_valid_row_data) is True
        assert value_level_metadata[0]["length_check"](mock_valid_row_data) is True
        assert value_level_metadata[0]["filter"](mock_filter_pass_row_data) is True
        assert value_level_metadata[0]["filter"](mock_filter_fail_row_data) is False


def test_extract_domain_metadata_not_found():
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    Test the case when requested domain is not found in Define XML.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        with pytest.raises(DomainNotFoundInDefineXMLError):
            reader.extract_domain_metadata(domain_name="not found domain")


def test_validate_schema():
    """
    Unit test for DefineXMLReader.validate_schema function.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        assert reader.validate_schema() is True


def test_extract_variable_metadata_not_found():
    """
    Unit test for DefineXMLReader.extract_domain_metadata function.
    Test the case when requested domain is not found in Define XML.
    """
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        with pytest.raises(DomainNotFoundInDefineXMLError):
            reader.extract_variables_metadata(domain_name="not found domain")


def test_get_domain_key_sequence():
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        ec_key_sequence: dict = reader.get_domain_key_sequence(domain_name="EC")
        assert ec_key_sequence == ["STUDYID", "USUBJID", "ECTRT", "ECSTDTC", "ECREASOC"]


def test_get_domain_key_sequence_for_supp():
    with open(test_define_file_path, "rb") as file:
        contents: bytes = file.read()
        reader = DefineXMLReaderFactory.from_file_contents(contents)
        suppec_key_sequence: dict = reader.get_domain_key_sequence(domain_name="SUPPEC")
        assert suppec_key_sequence == [
            "STUDYID",
            "RDOMAIN",
            "USUBJID",
            "IDVAR",
            "IDVARVAL",
            "QNAM",
        ]
