import unittest

import os

from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)


class TestCoreIssue390(unittest.TestCase):
    # NOTE: This is a plain unit test of DefineXMLReaderFactory's version
    # detection, not a CLI regression test, even though it lives in this folder.
    def test_get_define_version_from_define20(self):
        """Verify a Define-XML v2.0 file is correctly identified as version
        "2.0.0" by DefineXMLReaderFactory.get_define_version()."""
        path_to_define = os.path.join("tests", "resources", "test_defineV20-SDTM.xml")
        reader = DefineXMLReaderFactory.from_filename(path_to_define)
        self.assertEqual(reader.get_define_version(), "2.0.0")

    def test_get_define_version_from_define21(self):
        """Verify a Define-XML v2.1 file is correctly identified as version
        "2.1.0" by DefineXMLReaderFactory.get_define_version()."""
        path_to_define = os.path.join("tests", "resources", "test_defineV21-SDTM.xml")
        reader = DefineXMLReaderFactory.from_filename(path_to_define)
        self.assertEqual(reader.get_define_version(), "2.1.0")


if __name__ == "__main__":
    unittest.main()
