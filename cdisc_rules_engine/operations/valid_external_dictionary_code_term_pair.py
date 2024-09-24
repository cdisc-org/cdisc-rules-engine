from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.dictionaries.constants import DICTIONARY_VALIDATORS


class ValidExternalDictionaryCodeTermPair(BaseOperation):
    def _execute_operation(self):
        if self.params.external_dictionary_type not in DictionaryTypes.values():
            raise UnsupportedDictionaryType(
                f"{self.params.external_dictionary_type} is not supported by the engine"
            )

        validator_type = DICTIONARY_VALIDATORS.get(self.params.external_dictionary_type)
        if not validator_type:
            raise UnsupportedDictionaryType(
                f"{self.params.external_dictionary_type} is not supported by the "
                + "valid_external_dictionary_code_term_pair operation"
            )

        validator = validator_type(
            cache_service=self.cache,
            data_service=self.data_service,
            meddra_path=self.params.meddra_path,
            whodrug_path=self.params.whodrug_path,
            loinc_path=self.params.loinc_path,
            medrt_path=self.params.medrt_path,
        )

        return self.params.dataframe.apply(
            lambda row: validator.is_valid_code_term_pair(
                row,
                term_var=self.params.external_dictionary_term_variable,
                code_var=self.params.target,
            ),
            axis=1,
        )