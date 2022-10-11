from cdisc_rules_engine.models.dictionaries.whodrug import (
    DrugDictionary,
    WhodrugRecordTypes,
)
from .base_whodrug_term_serializer import BaseWhoDrugTermSerializer


class DrugDictionarySerializer(BaseWhoDrugTermSerializer):
    def __init__(self, term: DrugDictionary):
        self.__term = term
        super().__init__(term)

    @property
    def data(self) -> dict:
        return {
            **super(DrugDictionarySerializer, self).data,
            "drugName": self.__term.drugName,
        }

    @property
    def is_valid(self) -> bool:
        return (
            isinstance(self.__term.code, str)
            and len(self.__term.code) <= 6
            and isinstance(self.__term.drugName, str)
            and len(self.__term.drugName) <= 1500
            and self.__term.type == WhodrugRecordTypes.DRUG_DICT.value
        )
