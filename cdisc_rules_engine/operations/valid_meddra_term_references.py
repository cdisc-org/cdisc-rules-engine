import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface
from uuid import uuid4
from cdisc_rules_engine.models.dictionaries.meddra.meddra_variables import (
    MedDRAVariables,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm


class ValidMeddraTermReferences(OperationInterface):
    def execute(self) -> pd.DataFrame:
        # get metadata
        if not self.params.meddra_path:
            raise ValueError("Can't execute the operation, no meddra path provided")
        code_variables = [
            MedDRAVariables.SOC.value,
            MedDRAVariables.HLGT.value,
            MedDRAVariables.HLT.value,
            MedDRAVariables.DECOD.value,
            MedDRAVariables.LLT.value,
        ]
        code_strings = [
            f"{self.params.domain}{variable}" for variable in code_variables
        ]
        cache_key = f"meddra_valid_term_hierarchies_{self.params.meddra_path}"
        valid_term_hierarchies = self.cache.get(cache_key)
        if not valid_term_hierarchies:
            terms: dict = self.cache.get(self.params.meddra_path)
            valid_term_hierarchies = MedDRATerm.get_term_hierarchies(terms)
            self.cache.add(cache_key, valid_term_hierarchies)
        column = str(uuid4()) + "_terms"
        self.params.dataframe[column] = self.params.dataframe[code_strings].agg(
            "/".join, axis=1
        )
        result = self.params.dataframe[column].isin(valid_term_hierarchies)
        return self._handle_operation_result(result)
