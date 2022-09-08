from cdisc_rules_engine.operations.base_operation import BaseOperation
from uuid import uuid4
from cdisc_rules_engine.models.dictionaries.meddra.meddra_variables import (
    MedDRAVariables,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm


class MedDRACodeReferencesValidator(BaseOperation):
    def _execute_operation(self):
        # get metadata
        if not self.params.meddra_path:
            raise ValueError("Can't execute the operation, no meddra path provided")
        code_variables = [
            MedDRAVariables.SOCCD.value,
            MedDRAVariables.HLGTCD.value,
            MedDRAVariables.HLTCD.value,
            MedDRAVariables.PTCD.value,
            MedDRAVariables.LLTCD.value,
        ]
        code_strings = [
            f"{self.params.domain}{variable}" for variable in code_variables
        ]
        cache_key = f"meddra_valid_code_hierarchies_{self.params.meddra_path}"
        valid_code_hierarchies = self.cache.get(cache_key)
        if not valid_code_hierarchies:
            terms: dict = self.cache.get(self.params.meddra_path)
            valid_code_hierarchies = MedDRATerm.get_code_hierarchies(terms)
            self.cache.add(cache_key, valid_code_hierarchies)
        column = str(uuid4()) + "_codes"
        self.params.dataframe[column] = self.params.dataframe[code_strings].agg(
            "/".join, axis=1
        )
        result = self.params.dataframe[column].isin(valid_code_hierarchies)
        return result
