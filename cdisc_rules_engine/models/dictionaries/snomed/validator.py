from cdisc_rules_engine.interfaces.cache_service_interface import CacheServiceInterface
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.models.dictionaries.base_dictionary_validator import (
    BaseDictionaryValidator,
)
from cdisc_rules_engine.models.dictionaries.snomed.terms_factory import (
    SNOMEDTermsFactory,
)


class SNOMEDValidator(BaseDictionaryValidator):
    def __init__(
        self,
        data_service: DataServiceInterface = None,
        cache_service: CacheServiceInterface = None,
        dictionary_path: str = None,
        **kwargs,
    ):
        self.cache_service = cache_service
        self.data_service = data_service
        self.path = dictionary_path or kwargs.get("snomed_path")
        self.term_dictionary = kwargs.get("terms")
        self.terms_factory = SNOMEDTermsFactory(
            self.data_service,
            edition=self.path.get("edition"),
            version=self.path.get("version"),
            base_url=self.path.get("base_url"),
        )

    def get_term_dictionary(self, concepts=[]):
        if not self.term_dictionary:
            self.term_dictionary = self.terms_factory.install_terms(
                self.path, concepts=concepts
            )
        return self.term_dictionary

    def is_valid_code(
        self, code: str, term_type: str, variable: str, codes=[], **kwargs
    ) -> bool:
        term_dictionary = self.get_term_dictionary(concepts=codes)
        case_sensitive_check = kwargs.get("case_sensitive")
        if case_sensitive_check:
            return code in term_dictionary
        else:
            for key in term_dictionary:
                if key.lower() == code.lower():
                    return True
            return False

    def is_valid_code_term_pair(self, row, term_var, code_var, codes=[]) -> bool:
        term_dictionary = self.get_term_dictionary(concepts=codes)
        code = row[code_var]
        dictionary_term = term_dictionary.get(code)
        if not dictionary_term:
            return False
        return (
            row[term_var] == dictionary_term.preferred_term
            or row[term_var] == dictionary_term.full_name
        )
