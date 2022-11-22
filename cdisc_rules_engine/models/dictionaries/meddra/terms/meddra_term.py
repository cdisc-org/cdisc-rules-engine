from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.interfaces import DictionaryTermInterface


class MedDRATerm(DictionaryTermInterface):
    def __init__(self, record_params):
        self.code = record_params.get("code")
        self.term = record_params.get("term")
        self.term_type = record_params.get("type")
        self.abbreviation = record_params.get("abbreviation")
        self.parent_code = record_params.get("parentCode")
        self.parent_term = record_params.get("parentTerm")
        self.code_hierarchy = record_params.get("codeHierarchy")
        self.term_hierarchy = record_params.get("termHierarchy")

    def set_parent(self, parent: "MedDRATerm"):
        """
        Set parent code and term.
        """
        self.parent_code = parent.code
        self.parent_term = parent.term

    def get_identifier(self):
        return self.code

    @staticmethod
    def get_code_hierarchies(terms: dict) -> set:
        lowest_level_terms = terms[TermTypes.LLT.value].values()
        return set([term.code_hierarchy for term in lowest_level_terms])

    @staticmethod
    def get_term_hierarchies(terms: dict) -> set:
        lowest_level_terms = terms[TermTypes.LLT.value].values()
        return set([term.term_hierarchy for term in lowest_level_terms])

    @staticmethod
    def get_code_term_pairs(terms: dict) -> dict:
        code_term_pairs = {}
        for term_type in terms:
            code_term_pairs[term_type] = set(
                [(item.code, item.term) for item in terms[term_type].values()]
            )
        return code_term_pairs
