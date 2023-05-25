from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from .base_operation import BaseOperation
import os


class DefineVariableMetadata(BaseOperation):
    def _execute_operation(self):
        """
        Returns the specified metadata in the define for the specified target variable.
        Returns the metadata for all variables if no target variable specified.
        """
        define_contents = self.data_service.get_define_xml_contents(
            dataset_name=os.path.join(self.params.directory_path, DEFINE_XML_FILE_NAME)
        )
        define_reader = DefineXMLReaderFactory.from_file_contents(define_contents)
        variables_metadata = define_reader.extract_variables_metadata(
            self.params.domain
        )
        variable_metadata = {
            metadata["define_variable_name"]: metadata.get(self.params.value, "")
            for metadata in variables_metadata
        }
        return (
            variable_metadata.get(self.params.target, "")
            if self.params.target
            else variable_metadata
        )
