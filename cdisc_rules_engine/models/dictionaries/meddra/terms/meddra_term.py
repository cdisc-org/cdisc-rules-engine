from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes


class MedDRATerm:
    def __init__(self, record_params):
        self.code = record_params.get("code")
        self.term = record_params.get("term")
        self.term_type = record_params.get("type")
        self.dictionary_type = record_params.get("dictionaryType")
        self.dictionary_id = record_params.get("dictionaryId")
        self.abbreviation = record_params.get("abbreviation")
        self.parent_code = record_params.get("parentCode")
        self.parent_term = record_params.get("parentTerm")
        self.code_hierarchy = record_params.get("codeHierarchy")
        self.term_hierarchy = record_params.get("termHierarchy")

    def _ensure_valid_record_structure(self):
        assert isinstance(self.code, str)
        assert isinstance(self.term, str)
        assert TermTypes.contains(self.term_type)
        assert isinstance(self.dictionary_type, str)
        assert isinstance(self.dictionary_name, str)
        assert isinstance(self.dictionary_id, str)

    def _to_db_dict(self) -> dict:
        db_dict: dict = {
            "id": self.id,
            "code": self.code,
            "type": self.term_type,
            "term": self.term,
            "dictionaryId": self.dictionary_id,
            "dictionaryType": self.dictionary_type,
        }

        if self.abbreviation:
            db_dict["abbreviation"] = self.abbreviation

        if self.code_hierarchy:
            db_dict["codeHierarchy"] = self.code_hierarchy

        if self.term_hierarchy:
            db_dict["termHierarchy"] = self.term_hierarchy

        if self.parent_code:
            db_dict["parentCode"] = self.parent_code

        if self.parent_term:
            db_dict["parentTerm"] = self.parent_term

        return db_dict

    def set_parent(self, parent: "MedDRATerm"):
        """
        Set parent code and term.
        """
        self.parent_code = parent.code
        self.parent_term = parent.term

    @staticmethod
    def get_code_hierarchies(terms: dict) -> set:
        lowest_level_terms = terms[TermTypes.LLT.value]
        return set([term.code_hierarchy for term in lowest_level_terms])

    @staticmethod
    def get_term_hierarchies(terms: dict) -> set:
        lowest_level_terms = terms[TermTypes.LLT.value]
        return set([term.term_hierarchy for term in lowest_level_terms])

    @staticmethod
    def get_code_term_pairs(terms: dict) -> dict:
        code_term_pairs = {}
        for term_type in terms:
            code_term_pairs[term_type] = set(
                [(item.code, item.term) for item in terms[term_type]]
            )
        return code_term_pairs
