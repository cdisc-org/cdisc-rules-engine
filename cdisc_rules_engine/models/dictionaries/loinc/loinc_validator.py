from cdisc_rules_engine.interfaces.cache_service_interface import CacheServiceInterface
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.models.dictionaries.abstract_dictionary_validator import (
    AbstractDictionaryValidator,
)
from cdisc_rules_engine.models.dictionaries.base_external_dictionary import (
    ExternalDictionary,
)
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.dictionaries.get_dictionary_terms import (
    extract_dictionary_terms,
)


class LoincValidator(AbstractDictionaryValidator):
    def __init__(
        self,
        data_service: DataServiceInterface = None,
        cache_service: CacheServiceInterface = None,
        **kwargs,
    ):
        self.cache_service = cache_service
        self.data_service = data_service
        self.path = kwargs.get("loinc_path")
        self.term_dictionary = kwargs.get("terms")

    def get_term_dictionary(self) -> ExternalDictionary:
        if self.term_dictionary:
            return self.term_dictionary

        if self.cache_service is None:
            raise Exception(
                "External Dictionary validation requires cache access, none found"
            )

        terms_dictionary = self.cache_service.get(self.path)
        if not terms_dictionary:
            if self.data_service is None:
                raise Exception(
                    "External Dictionary validation requires data service. None found"
                )
            terms_dictionary = extract_dictionary_terms(
                self.data_service, DictionaryTypes.LOINC, self.path
            )
        self.term_dictionary = terms_dictionary
        self.cache_service.add(self.path, terms_dictionary)

        return self.term_dictionary

    def is_valid_term(
        self, term: str, term_type: str = "", variable: str = "", **kwargs
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
        term_dictionary = self.get_term_dictionary()
        case_sensitive_check = kwargs.get("case_sensitive")
        if case_sensitive_check:
            print("here")
            print(term in term_dictionary)
            return term in term_dictionary
        else:
            for key in term_dictionary:
                if key.lower() == term.lower():
                    return True
            return False
