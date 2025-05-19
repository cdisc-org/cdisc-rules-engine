from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset


class ValidExternalDictionaryValue(BaseOperation):
    def _execute_operation(self):
        if not isinstance(self.params.dataframe, DaskDataset):
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
                lambda row: validator.is_valid_term(
                    term=row[self.params.target],
                    term_type=self.params.dictionary_term_type,
                    variable=self.params.original_target,
                ),
                axis=1,
            )

        # Dask cannot serialize lock objects, so we build a validation lookup table
        # and use it in a map function
        target_col = self.params.target
        operation_id = self.params.operation_id
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
        unique_terms = self.params.dataframe[target_col].unique()
        validation_dict = {}
        for term in unique_terms:
            validation_dict[term] = validator.is_valid_term(
                term=term,
                term_type=self.params.dictionary_term_type,
                variable=self.params.original_target,
            )

        def check_term(term):
            return validation_dict.get(term, False)

        result = self.params.dataframe._data[target_col].map(check_term)
        result.name = operation_id
        return result
