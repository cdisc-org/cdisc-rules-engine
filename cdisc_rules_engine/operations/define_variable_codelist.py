from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from .base_operation import BaseOperation
import os


class DefineVariableCodelist(BaseOperation):
    def _execute_operation(self):
        """
        Returns the codelist specified in the define for the specified target variable
        """
        define_contents = self.data_service.get_define_xml_contents(
            dataset_name=os.path.join(self.params.directory_path, DEFINE_XML_FILE_NAME)
        )
        define_reader = DefineXMLReaderFactory.from_file_contents(define_contents)
        variables_metadata = define_reader.extract_variables_metadata(
            self.params.domain
        )
        variable_metadata = next(
            iter(
                [
                    metadata
                    for metadata in variables_metadata
                    if metadata["define_variable_name"] == self.params.target
                ]
            ),
            {},
        )
        return variable_metadata.get("define_variable_ccode", "")
