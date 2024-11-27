from cdisc_rules_engine.operations.base_operation import BaseOperation


class ValidExternalDictionaryCodeTermPair(BaseOperation):
    def _execute_operation(self):
        validator_type = (
            self.params.external_dictionaries.get_dictionary_validator_class(
                self.params.external_dictionary_type
            )
        )
        validator = validator_type(
            cache_service=self.cache,
            data_service=self.data_service,
            dictionary_path=self.params.external_dictionaries.get_dictionary_path(
                self.params.external_dictionary_type
            ),
        )

        return self.params.dataframe.apply(
            lambda row: validator.is_valid_code_term_pair(
                row,
                term_var=self.params.external_dictionary_term_variable,
                code_var=self.params.target,
                codes=self.params.dataframe[self.params.target].unique(),
            ),
            axis=1,
        )
