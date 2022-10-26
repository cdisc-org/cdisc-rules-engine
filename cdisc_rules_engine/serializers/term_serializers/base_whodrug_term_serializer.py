from abc import ABC

from cdisc_rules_engine.models.dictionaries.whodrug.base_whodrug_term import (
    BaseWhoDrugTerm,
)
from cdisc_rules_engine.serializers import BaseSerializer


class BaseWhoDrugTermSerializer(BaseSerializer, ABC):
    def __init__(self, term: BaseWhoDrugTerm):
        self.__term = term

    @property
    def data(self) -> dict:
        return {"code": self.__term.code, "type": self.__term.type}
