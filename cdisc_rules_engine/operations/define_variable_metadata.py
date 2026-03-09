import os.path

from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from .base_operation import BaseOperation
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME


class DefineVariableMetadata(BaseOperation):
    def _execute_operation(self):
        """
        If a target variable is specified, returns the specified metadata in the define
          for the specified target variable.
        For example:
            Input:
                - operator: define_variable_metadata
                  attribute_name: define_variable_label
                  name: LBTESTCD
            Output:
                "Laboratory Test Code"
        If no target variable specified, returns a dictionary containing the specified
          metadata in the define for all variables.
        For example:
            Input:
                - operator: define_variable_metadata
                  attribute_name: define_variable_label
            Output:
                {
                    "STUDYID": "Study Identifier",
                    "USUBJID": "Unique Subject Identifier",
                    "LBTESTCD": "Laboratory Test Code"
                    ...
                }
        """
        define_path = (
            self.params.define_xml_path
            if self.params.define_xml_path
            else os.path.join(self.params.directory_path, DEFINE_XML_FILE_NAME)
        )
        if not os.path.exists(define_path):
            raise FileNotFoundError("Define XML file %s not found", define_path)
        define_contents = self.data_service.get_define_xml_contents(
            dataset_name=define_path
        )
        define_reader = DefineXMLReaderFactory.from_file_contents(define_contents)
        variables_metadata = define_reader.extract_variables_metadata(
            self.params.domain
        )
        variable_metadata = {
            metadata["define_variable_name"]: metadata.get(
                self.params.attribute_name, ""
            )
            for metadata in variables_metadata
        }
        return (
            variable_metadata.get(
                self.params.target.replace("--", self.params.domain, 1), ""
            )
            if self.params.target
            else variable_metadata
        )
