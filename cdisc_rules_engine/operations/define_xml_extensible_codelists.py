import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation

# from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
# from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
#     DefineXMLReaderFactory,
# )
# import os


class DefineCodelists(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns a list of codelist values from the define.xml file.
        fxn to be be used when a codelist is extensible to acquire the additional values
        """
        # TODO: extensible is populated, parse and return the list of terms
        # optional codelist and returntype -- allow for sub val or ccode checks
        # if self.params.codelists:
        #     codelists = self.params.codelists
        # if self.params.returntype:
        #     check = self.params.returntype
        ct_package_data = next(
            iter(self.library_metadata._ct_package_metadata.values())
        )
        return ct_package_data

        # define_contents = self.data_service.get_define_xml_contents(
        #     dataset_name=os.path.join(self.params.directory_path, DEFINE_XML_FILE_NAME)
        # )
        # define_reader = DefineXMLReaderFactory.from_file_contents(define_contents)
        # extensible_codelists = define_reader.get_extensible_codelist_mappings()
        # return extensible_codelists
