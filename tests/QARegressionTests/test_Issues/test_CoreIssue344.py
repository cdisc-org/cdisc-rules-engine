import unittest
import os
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)

"""Tests taken from unittests, written by @nhaydel.
The unit tests are good enough to automate the valdation of accpetance criteria
of issue 344"""


class TestCoreIssue344(unittest.TestCase):
    def test_get_domain_key_sequence(self):
        test_define_file_path = os.path.join(
            "tests", "resources", "test_defineV22-SDTM.xml"
        )
        with open(test_define_file_path, "rb") as file:
            contents: bytes = file.read()
            reader = DefineXMLReaderFactory.from_file_contents(contents)
            ec_key_sequence: dict = reader.get_domain_key_sequence(domain_name="EC")
            assert ec_key_sequence == [
                "STUDYID",
                "USUBJID",
                "ECTRT",
                "ECSTDTC",
                "ECREASOC",
            ]

    def test_get_domain_key_sequence_for_supp(self):
        test_define_file_path = os.path.join(
            "tests", "resources", "test_defineV22-SDTM.xml"
        )
        with open(test_define_file_path, "rb") as file:
            contents: bytes = file.read()
            reader = DefineXMLReaderFactory.from_file_contents(contents)
            suppec_key_sequence: dict = reader.get_domain_key_sequence(
                domain_name="SUPPEC"
            )
            assert suppec_key_sequence == [
                "STUDYID",
                "RDOMAIN",
                "USUBJID",
                "IDVAR",
                "IDVARVAL",
                "QNAM",
            ]


if __name__ == "__main__":
    unittest.main()
