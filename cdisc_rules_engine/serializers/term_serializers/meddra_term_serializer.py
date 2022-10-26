from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.serializers import BaseSerializer


class MedDRATermSerializer(BaseSerializer):
    def __init__(self, term: MedDRATerm):
        self.__term = term

    @property
    def data(self) -> dict:
        data: dict = {
            "code": self.__term.code,
            "type": self.__term.term_type,
            "term": self.__term.term,
        }
        if self.__term.abbreviation:
            data["abbreviation"] = self.__term.abbreviation
        if self.__term.code_hierarchy:
            data["codeHierarchy"] = self.__term.code_hierarchy
        if self.__term.term_hierarchy:
            data["termHierarchy"] = self.__term.term_hierarchy
        if self.__term.parent_code:
            data["parentCode"] = self.__term.parent_code
        if self.__term.parent_term:
            data["parentTerm"] = self.__term.parent_term
        return data

    @property
    def is_valid(self) -> bool:
        return (
            isinstance(self.__term.code, str)
            and isinstance(self.__term.term, str)
            and TermTypes.contains(self.__term.term_type)
        )
