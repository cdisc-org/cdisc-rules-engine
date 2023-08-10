from abc import ABC, abstractmethod


class AbstractDictionaryValidator(ABC):
    @abstractmethod
    def is_valid_term(
        self,
        term_dictionary: dict,
        term: str,
        term_type: str = "",
        variable: str = "",
        **kwargs
    ) -> bool:
        """
        Method to identify whether a term is valid based on its term type.

        Args:
            term_dictionary: The dictionary of available terms.
            term: The dictionary term used
            term_type: The component of the dictionary to validate against.
            variable: The source variable of the term value.
            kwargs: Additional validator specific variables

        Returns:
            True: The term is valid
            False: The term is not valid

            Note: The definition of "valid" may be different for each dictionary.

        """
        pass

    @abstractmethod
    def get_term_dictionary(self) -> dict:
        """
        Returns the term dictionary for the validator.
        If the dictionary is not installed it will install and
        cache the appropriate dictionary

        Returns:
            A dictionary of terms representative of the external dictionary
        """
        pass
