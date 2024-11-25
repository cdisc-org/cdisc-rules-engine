from cdisc_rules_engine.interfaces.cache_service_interface import CacheServiceInterface
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.models.dictionaries.base_dictionary_validator import (
    BaseDictionaryValidator,
)
from cdisc_rules_engine.models.dictionaries.medrt.term import MEDRTConcept
from cdisc_rules_engine.models.dictionaries.medrt.terms_factory import MEDRTTermsFactory


class MEDRTValidator(BaseDictionaryValidator):
    def __init__(
        self,
        data_service: DataServiceInterface = None,
        cache_service: CacheServiceInterface = None,
        dictionary_path: str = None,
        **kwargs,
    ):
        self.cache_service = cache_service
        self.data_service = data_service
        self.path = dictionary_path or kwargs.get("medrt_path")
        self.term_dictionary = kwargs.get("terms")
        self.terms_factory = MEDRTTermsFactory(self.data_service)

    def is_valid_term(self, term: str, term_type: str, variable: str, **kwargs) -> bool:
        term_dictionary = self.get_term_dictionary()
        case_sensitive_check = kwargs.get("case_sensitive")
        all_terms = set([term.name for term in term_dictionary.values()])
        if case_sensitive_check:
            return term in all_terms
        else:
            for dictionary_term in all_terms:
                if dictionary_term.lower() == term.lower():
                    return True
            return False

    def is_valid_code(
        self, code: str, term_type: str, variable: str, codes=[], **kwargs
    ) -> bool:
        term_dictionary = self.get_term_dictionary()
        case_sensitive_check = kwargs.get("case_sensitive")
        if case_sensitive_check:
            return code in term_dictionary
        else:
            for key in term_dictionary:
                if key.lower() == code.lower():
                    return True
            return False

    def is_valid_code_term_pair(self, row, term_var, code_var, codes=[]) -> bool:
        term_dictionary = self.get_term_dictionary()
        code = row[code_var]
        dictionary_term = term_dictionary.get(code)
        if not dictionary_term:
            return False
        valid = row[term_var] == dictionary_term.name
        if isinstance(dictionary_term, MEDRTConcept):
            # Check the synonyms against the concept code
            for synonym in dictionary_term.synonyms:
                term = term_dictionary.get(synonym)
                if term:
                    valid = valid or term.name == row[term_var]
        return valid
