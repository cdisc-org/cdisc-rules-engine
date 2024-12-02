import os
from typing import List, Optional

from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)


def get_define_version(dataset_paths: List[str]) -> Optional[str]:
    """
    Method used to extract define version from xml file located
     in the same directory with datasets
    """
    if not dataset_paths:
        return None
    if dataset_paths[0].endswith(".xml"):
        define_xml_reader = DefineXMLReaderFactory.from_filename(dataset_paths[0])
        return define_xml_reader.get_define_version()
    path_to_data = os.path.dirname(dataset_paths[0])
    if not path_to_data or DEFINE_XML_FILE_NAME not in os.listdir(path_to_data):
        return None
    path_to_define = os.path.join(path_to_data, DEFINE_XML_FILE_NAME)
    define_xml_reader = DefineXMLReaderFactory.from_filename(path_to_define)
    version = define_xml_reader.get_define_version()
    return version


def get_define_ct(dataset_paths: List[str], define_version) -> Optional[str]:
    """
    Method used to extract CT version from define xml file located
    in the same directory with datasets
    """
    if not dataset_paths:
        return None
    ct = None
    if dataset_paths[0].endswith(".xml") and define_version == "2.1.0":
        define_xml_reader = DefineXMLReaderFactory.from_filename(dataset_paths[0])
        return define_xml_reader.get_ct_version()
    path_to_data = os.path.dirname(dataset_paths[0])
    if not path_to_data or DEFINE_XML_FILE_NAME not in os.listdir(path_to_data):
        return None
    path_to_define = os.path.join(path_to_data, DEFINE_XML_FILE_NAME)
    define_xml_reader = DefineXMLReaderFactory.from_filename(path_to_define)
    if define_version == "2.1.0":
        ct = define_xml_reader.get_ct_version()
    return ct
