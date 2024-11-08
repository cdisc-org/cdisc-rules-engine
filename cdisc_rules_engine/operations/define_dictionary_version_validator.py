from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.models.external_dictionaries_container import (
    DICTIONARY_VALIDATORS,
    DictionaryTypes,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from .base_operation import BaseOperation
from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType
import os


class DefineDictionaryVersionValidator(BaseOperation):
    def _execute_operation(self) -> bool:
        """
        Compares version of an external dictionary defined in the define.xml with the version
        of provided when initiating validation.
        """
        if self.params.external_dictionary_type not in DictionaryTypes.values():
            raise UnsupportedDictionaryType(
                f"{self.params.external_dictionary_type} is not supported by the engine"
            )

        validator_type = DICTIONARY_VALIDATORS.get(self.params.external_dictionary_type)
        if not validator_type:
            raise UnsupportedDictionaryType(
                f"{self.params.external_dictionary_type} is not supported by the "
                + "define_dictionary_version_validator operation"
            )

        validator = validator_type(
            cache_service=self.cache,
            data_service=self.data_service,
            meddra_path=self.params.meddra_path,
            whodrug_path=self.params.whodrug_path,
            loinc_path=self.params.loinc_path,
        )
        define_contents = self.data_service.get_define_xml_contents(
            dataset_name=os.path.join(self.params.directory_path, DEFINE_XML_FILE_NAME)
        )
        define_reader = DefineXMLReaderFactory.from_file_contents(define_contents)
        define_dictionary_version = define_reader.get_external_dictionary_version(
            self.params.external_dictionary_type
        )
        dictionary_version = validator.get_dictionary_version()

        return dictionary_version == define_dictionary_version
