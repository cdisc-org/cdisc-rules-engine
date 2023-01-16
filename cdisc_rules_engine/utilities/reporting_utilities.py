import os

from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.services.define_xml_reader import DefineXMLReader


def get_define_version_from_data(dataset_paths: str):
    """
    Method used to extract define version from xml file located
     in the same directory with datasets
    """
    if not dataset_paths:
        return None
    path_to_data = os.path.dirname(dataset_paths[0])
    if DEFINE_XML_FILE_NAME not in os.listdir(path_to_data):
        return None
    path_to_define = "/".join([path_to_data, DEFINE_XML_FILE_NAME])
    define_xml_reader = DefineXMLReader.from_filename(path_to_define)
    define_xml_reader.read()
    metadata = define_xml_reader._odm_loader.loader.parser.mdv[0].attrib

    for key, val in metadata.items():
        if key.endswith("DefineVersion"):
            return val
