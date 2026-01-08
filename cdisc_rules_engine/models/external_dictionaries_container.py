from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.dictionaries.loinc.loinc_validator import LoincValidator
from cdisc_rules_engine.models.dictionaries.meddra.meddra_validator import (
    MedDRAValidator,
)
from cdisc_rules_engine.models.dictionaries.medrt.validator import MEDRTValidator
from cdisc_rules_engine.models.dictionaries.snomed.validator import SNOMEDValidator
from cdisc_rules_engine.models.dictionaries.unii.validator import UNIIValidator
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_validator import (
    WhoDrugValidator,
)
from cdisc_rules_engine.models.dictionaries.base_dictionary_validator import (
    BaseDictionaryValidator,
)
from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType


DICTIONARY_VALIDATORS = {
    DictionaryTypes.MEDDRA.value: MedDRAValidator,
    DictionaryTypes.LOINC.value: LoincValidator,
    DictionaryTypes.WHODRUG.value: WhoDrugValidator,
    DictionaryTypes.MEDRT.value: MEDRTValidator,
    DictionaryTypes.UNII.value: UNIIValidator,
    DictionaryTypes.SNOMED.value: SNOMEDValidator,
}


class ExternalDictionariesContainer:
    def __init__(self, dictionary_path_mapping={}):
        self.dictionary_path_mapping = dictionary_path_mapping

    def is_valid_external_dictionary(self, dictionary_type: str) -> bool:
        return dictionary_type in DictionaryTypes.values()

    def get_dictionary_path(self, dictionary_type: str) -> str:
        if not self.is_valid_external_dictionary(dictionary_type):
            raise UnsupportedDictionaryType(
                f"{dictionary_type} is not supported by the engine"
            )
        return self.dictionary_path_mapping.get(dictionary_type)

    def get_dictionary_validator_class(
        self, dictionary_type: str
    ) -> BaseDictionaryValidator:
        validator_type = DICTIONARY_VALIDATORS.get(dictionary_type)
        if not validator_type:
            raise UnsupportedDictionaryType(
                f"{dictionary_type} has no associated dictionary validator "
            )
        return validator_type
