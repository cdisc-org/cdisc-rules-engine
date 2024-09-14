from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.dictionaries.loinc.loinc_validator import LoincValidator
from cdisc_rules_engine.models.dictionaries.meddra.meddra_validator import (
    MedDRAValidator,
)
from cdisc_rules_engine.models.dictionaries.medrt.validator import MEDRTValidator
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_validator import (
    WhoDrugValidator,
)


DICTIONARY_VALIDATORS = {
    DictionaryTypes.MEDDRA.value: MedDRAValidator,
    DictionaryTypes.LOINC.value: LoincValidator,
    DictionaryTypes.WHODRUG.value: WhoDrugValidator,
    DictionaryTypes.MEDRT.value: MEDRTValidator,
}
