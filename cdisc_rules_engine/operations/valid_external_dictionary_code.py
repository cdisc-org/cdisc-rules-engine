from cdisc_rules_engine.operations.base_operation import BaseOperation


class ValidExternalDictionaryCode(BaseOperation):
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
            lambda row: validator.is_valid_code(
                code=row[self.params.target],
                term_type=self.params.dictionary_term_type,
                variable=self.params.original_target,
                codes=self.params.dataframe[self.params.target].unique(),
            ),
            axis=1,
        )
