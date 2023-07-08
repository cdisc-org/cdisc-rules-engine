import unittest

import os

from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)


class TestCoreIssue390(unittest.TestCase):
    def test_get_define_version_from_define20(self):
        path_to_define = os.path.join("tests", "resources", "test_defineV20-SDTM.xml")
        reader = DefineXMLReaderFactory.from_filename(path_to_define)
        print("\n\n\n", reader.get_define_version(), "\n\n\n")
        self.assertEqual(reader.get_define_version(), "2.0.0")

    def test_get_define_version_from_define21(self):
        path_to_define = os.path.join("tests", "resources", "test_defineV21-SDTM.xml")
        reader = DefineXMLReaderFactory.from_filename(path_to_define)
        self.assertEqual(reader.get_define_version(), "2.1.0")


if __name__ == "__main__":
    unittest.main()
