from cdisc_rules_engine.exceptions.custom_exceptions import UnsupportedDictionaryType
from cdisc_rules_engine.models.dictionaries.meddra.meddra_validator import (
    MedDRAValidator,
)
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes


class ValidExternalDictionaryValue(BaseOperation):
    def _execute_operation(self):
        if self.params.external_dictionary_type not in DictionaryTypes.values():
            raise UnsupportedDictionaryType(
                f"{self.params.external_dictionary_type} is not supported by the engine"
            )

        validator_map = {DictionaryTypes.MEDDRA.value: MedDRAValidator}

        validator_type = validator_map.get(self.params.external_dictionary_type)
        if not validator_type:
            raise UnsupportedDictionaryType(
                f"{self.params.external_dictionary_type} is not supported by the "
                + "valid_external_dictionary_value operation"
            )

        validator = validator_type(
            cache_service=self.cache,
            meddra_path=self.params.meddra_path,
            whodrug_path=self.params.whodrug_path,
        )

        return self.params.dataframe.apply(
            lambda row: validator.is_valid_term(
                term=row[self.params.target],
                term_type=self.params.dictionary_term_type,
                variable=self.params.original_target,
            ),
            axis=1,
        )
