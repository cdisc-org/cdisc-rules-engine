from cdisc_rules_engine.models.dictionaries.whodrug import (
    AtcClassification,
    WhodrugRecordTypes,
)
from .base_whodrug_term_serializer import BaseWhoDrugTermSerializer


class AtcClassificationSerializer(BaseWhoDrugTermSerializer):
    def __init__(self, term: AtcClassification):
        self.__term = term
        super().__init__(term)

    @property
    def data(self) -> dict:
        return {
            **super(AtcClassificationSerializer, self).data,
            "parentCode": self.__term.parentCode,
        }

    @property
    def is_valid(self) -> bool:
        return (
            isinstance(self.__term.parentCode, str)
            and len(self.__term.parentCode) <= 6
            and isinstance(self.__term.code, str)
            and len(self.__term.code) <= 7
            and self.__term.type == WhodrugRecordTypes.ATC_CLASSIFICATION.value
        )
