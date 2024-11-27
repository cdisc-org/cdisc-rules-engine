from cdisc_rules_engine.interfaces.cache_service_interface import CacheServiceInterface
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.models.dictionaries.base_dictionary_validator import (
    BaseDictionaryValidator,
)
from cdisc_rules_engine.models.dictionaries.loinc.loinc_terms_factory import (
    LoincTermsFactory,
)


class LoincValidator(BaseDictionaryValidator):
    def __init__(
        self,
        data_service: DataServiceInterface = None,
        cache_service: CacheServiceInterface = None,
        dictionary_path: str = None,
        **kwargs,
    ):
        self.cache_service = cache_service
        self.data_service = data_service
        self.path = dictionary_path or kwargs.get("loinc_path")
        self.term_dictionary = kwargs.get("terms")
        self.terms_factory = LoincTermsFactory(self.data_service)

    def is_valid_term(
        self, term: str, term_type: str = "", variable: str = "", codes=[], **kwargs
    ) -> bool:
        """
        Method to identify whether a term is valid based on its term type.

        Args:
            term: The dictionary term used
            term_type: The term type to validate against
            variable: The variable used to source the term data
            kwargs: Additional validator specific variables

        Returns:
            True: The term is valid
            False: The term is not valid
        """
        return self.is_valid_code(term, term_type, variable, **kwargs)

    def is_valid_code(
        self, code: str, term_type: str = "", variable: str = "", codes=[], **kwargs
    ) -> bool:
        """
        Method to identify whether a term is valid based on its term type.

        Args:
            code: The dictionary code used
            term_type: The term type to validate against
            variable: The variable used to source the term data
            kwargs: Additional validator specific variables

        Returns:
            True: The term is valid
            False: The term is not valid
        """
        term_dictionary = self.get_term_dictionary()
        case_sensitive_check = kwargs.get("case_sensitive")
        if case_sensitive_check:
            return code in term_dictionary
        else:
            for key in term_dictionary:
                if key.lower() == code.lower():
                    return True
            return False
