from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset


class ValidExternalDictionaryCodeTermPair(BaseOperation):
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
                lambda row: validator.is_valid_code_term_pair(
                    row,
                    term_var=self.params.external_dictionary_term_variable,
                    code_var=self.params.target,
                    codes=self.params.dataframe[self.params.target].unique(),
                ),
                axis=1,
            )

        # Dask cannot serialize lock objects, so we build a validation lookup table
        # and use it in a map function
        target_col = self.params.target
        term_var = self.params.external_dictionary_term_variable
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
        unique_codes = self.params.dataframe[target_col].unique()
        df_computed = self.params.dataframe._data.compute()
        unique_pairs = df_computed[[target_col, term_var]].drop_duplicates()
        validation_dict = {}
        for _, row in unique_pairs.iterrows():
            code = row[target_col]
            term = row[term_var]
            key = (code, term)
            mock_row = {target_col: code, term_var: term}
            validation_dict[key] = validator.is_valid_code_term_pair(
                mock_row,
                term_var=term_var,
                code_var=target_col,
                codes=unique_codes,
            )

        def validate_pair(df):
            result = df.apply(
                lambda row: validation_dict.get(
                    (row[target_col], row[term_var]), False
                ),
                axis=1,
            )
            return result

        result = self.params.dataframe._data.map_partitions(validate_pair)
        result.name = operation_id
        return result
