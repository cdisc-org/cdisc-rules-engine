from cdisc_rules_engine.exceptions.custom_exceptions import InvalidDictionaryVariable
from cdisc_rules_engine.interfaces.cache_service_interface import CacheServiceInterface
from cdisc_rules_engine.interfaces.data_service_interface import DataServiceInterface
from cdisc_rules_engine.models.dictionaries.abstract_dictionary_validator import (
    AbstractDictionaryValidator,
)
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.dictionaries.meddra.meddra_variables import (
    MedDRAVariables,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.models.dictionaries.get_dictionary_terms import (
    extract_dictionary_terms,
)


class MedDRAValidator(AbstractDictionaryValidator):
    def __init__(
        self,
        data_service: DataServiceInterface = None,
        cache_service: CacheServiceInterface = None,
        **kwargs,
    ):
        self.code_variables = set(
            [
                f"--{MedDRAVariables.PTCD.value}",
                f"--{MedDRAVariables.LLTCD.value}",
                f"--{MedDRAVariables.HLTCD.value}",
                f"--{MedDRAVariables.HLGTCD.value}",
                f"--{MedDRAVariables.LLTCD.value}",
                f"--{MedDRAVariables.SOCCD.value}",
                f"--{MedDRAVariables.BDSYSCD.value}",
            ]
        )
        self.cache_service = cache_service
        self.data_service = data_service
        self.path = kwargs.get("meddra_path")
        self.term_dictionary = kwargs.get("terms")

    def get_term_dictionary(self) -> dict:
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
                self.data_service, DictionaryTypes.MEDDRA, self.path
            )
        self.term_dictionary = terms_dictionary
        self.cache_service.add(self.path, terms_dictionary)

        return self.term_dictionary

    def is_valid_term(self, term: str, term_type: str, variable: str, **kwargs) -> bool:
        """
        Method to identify whether a term is valid based on its term type.

        Args:
            term_dictionary: The dictionary of available terms. Ex:
                {
                    "soc": {
                        <soc term code>: instance of MedDRATerm
                        ...
                    },
                    "hlt": {
                        <high level term code>: instance of MedDRATerm
                        ...
                    }
                    ...
                }
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
        term_type = term_type.lower()
        if term_type not in TermTypes.values():
            raise InvalidDictionaryVariable(
                f"{term_type} does not correspond to a MedDRA term type"
            )

        if variable in self.code_variables:
            return term in term_dictionary.get(term_type, {})

        all_terms = term_dictionary.get(term_type, {}).values()
        if case_sensitive_check:
            valid_terms = [
                meddra_term for meddra_term in all_terms if term == meddra_term.term
            ]
        else:
            valid_terms = [
                meddra_term
                for meddra_term in all_terms
                if term.lower() == meddra_term.term.lower()
            ]

        return len(valid_terms) > 0
