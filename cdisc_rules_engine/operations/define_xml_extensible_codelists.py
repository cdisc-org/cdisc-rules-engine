import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
import os


class DefineCodelists(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns a list of codelist values from the define.xml file.
        fxn to be be used when a codelist is extensible to acquire the additional values
        """
        # get define xml - currently expects file named define.xml
        define_contents = self.data_service.get_define_xml_contents(
            dataset_name=os.path.join(self.params.directory_path, DEFINE_XML_FILE_NAME)
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
