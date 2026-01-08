class BaseDictionaryValidator:
    def is_valid_term(
        self, term: str, term_type: str = "", variable: str = "", **kwargs
    ) -> bool:
        """
        Method to identify whether a term is valid based on its term type.

        Args:
            term: The dictionary term used
            term_type: The component of the dictionary to validate against.
            variable: The source variable of the term value.
            kwargs: Additional validator specific variables

        Returns:
            True: The term is valid
            False: The term is not valid

            Note: The definition of "valid" may be different for each dictionary.

        """
        raise NotImplementedError

    def is_valid_code(
        self, code: str, term_type: str = "", variable: str = "", codes=[], **kwargs
    ) -> bool:
        """
        Method to identify whether a code is valid based on its term type.

        Args:
            code: The dictionary code used
            term_type: The component of the dictionary to validate against.
            variable: The source variable of the term value.
            kwargs: Additional validator specific variables

        Returns:
            True: The code is valid
            False: The code is not valid

            Note: The definition of "valid" may be different for each dictionary.

        """
        raise NotImplementedError

    def is_valid_code_term_pair(self, row, term_var, code_var, codes=[]) -> bool:
        """
        Method to identify whether a term in a dictionary matches the code used to specify it.

        Args:
            row: the dataframe row
            term_var: The variable containing the term text
            code_var: The variable containing the term code
        Returns:
            True: The code matches the term.
            False: The code does not match the term.

            Note: The definition of "matches" may be different for each dictionary.
        """
        raise NotImplementedError

    def get_dictionary_version(self) -> str:
        return self.terms_factory.get_version(self.path)

    def get_term_dictionary(self):
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
            terms_dictionary = self.terms_factory.install_terms(self.path)
        self.term_dictionary = terms_dictionary
        self.cache_service.add(self.path, terms_dictionary)

        return self.term_dictionary
