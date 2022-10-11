from cdisc_rules_engine.models.dictionaries.whodrug import AtcText, WhodrugRecordTypes
from .base_whodrug_term_serializer import BaseWhoDrugTermSerializer


class AtcTextSerializer(BaseWhoDrugTermSerializer):
    def __init__(self, term: AtcText):
        self.__term = term
        super(AtcTextSerializer, self).__init__(term)

    @property
    def data(self) -> dict:
        return {
            **super(AtcTextSerializer, self).data,
            "parentCode": self.__term.parentCode,
            "level": self.__term.level,
            "text": self.__term.text,
        }

    @property
    def is_valid(self) -> bool:
        return (
            isinstance(self.__term.parentCode, str)
            and len(self.__term.parentCode) <= 7
            and isinstance(self.__term.level, int)
            and self.__term.level in range(1, 5)
            and len(str(self.__term.level)) == 1
            and isinstance(self.__term.text, str)
            and len(self.__term.text) <= 110
            and self.__term.type == WhodrugRecordTypes.ATC_TEXT.value
        )
