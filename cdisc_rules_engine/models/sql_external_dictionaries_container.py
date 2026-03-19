from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.readers.external_dictionary_readers.whodrug_reader import WhoDrugReader
from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType


IMPLEMENTED_DICTIONARY_VALIDATORS = {
    DictionaryTypes.WHODRUG.value: WhoDrugReader,
}

UNIMPLEMENTED_DICTIONARIES = [
    DictionaryTypes.MEDDRA.value,
    DictionaryTypes.LOINC.value,
    DictionaryTypes.MEDRT.value,
    DictionaryTypes.UNII.value,
    DictionaryTypes.SNOMED.value,
]


class SqlExternalDictionariesContainer:
    def __init__(self, dictionary_path_mapping={}):
        self.dictionary_path_mapping = dictionary_path_mapping

    def get_all_implemented_reader_classes(self) -> dict:
        valid_reader_classes = {}
        for dictionary_type, path in self.dictionary_path_mapping.items():
            if path:
                if self.is_valid_external_dictionary(dictionary_type) and self.is_implemented_external_dictionary(
                    dictionary_type
                ):
                    valid_reader_classes[dictionary_type] = IMPLEMENTED_DICTIONARY_VALIDATORS[dictionary_type]
        return valid_reader_classes

    def get_dictionary_path(self, dictionary_type: str) -> str:
        if not self.is_valid_external_dictionary(dictionary_type):
            raise UnsupportedDictionaryType(dictionary_type)
        return self.dictionary_path_mapping[dictionary_type]

    def get_dictionary_reader_class(self, dictionary_type: str):
        if not self.is_implemented_external_dictionary(dictionary_type):
            raise UnsupportedDictionaryType(dictionary_type)
        return IMPLEMENTED_DICTIONARY_VALIDATORS[dictionary_type]

    @staticmethod
    def is_valid_external_dictionary(dictionary_type: str) -> bool:
        return dictionary_type in DictionaryTypes.values()

    @staticmethod
    def is_implemented_external_dictionary(dictionary_type: str) -> bool:
        return dictionary_type in IMPLEMENTED_DICTIONARY_VALIDATORS
