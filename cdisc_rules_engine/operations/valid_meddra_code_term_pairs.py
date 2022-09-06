from cdisc_rules_engine.operations.base_operation import BaseOperation
from uuid import uuid4
from cdisc_rules_engine.models.dictionaries.meddra.meddra_variables import (
    MedDRAVariables,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.utilities.utils import get_meddra_code_term_pairs_cache_key
from typing import Optional, Tuple


class ValidMeddraCodeTermPairs(BaseOperation):
    def _execute_operation(self):
        # get metadata
        if not self.params.meddra_path:
            raise ValueError("Can't execute the operation, no meddra path provided")
        term_type, columns = self._get_columns_by_meddra_variable_name()
        cache_key: str = get_meddra_code_term_pairs_cache_key(self.params.meddra_path)
        valid_code_term_pairs = self.cache.get(cache_key)
        if not valid_code_term_pairs:
            terms: dict = self.cache.get(self.params.meddra_path)
            valid_code_term_pairs = MedDRATerm.get_code_term_pairs(terms)
            self.cache.add(cache_key, valid_code_term_pairs)
        column = str(uuid4()) + "_pairs"
        self.params.dataframe[column] = list(
            zip(
                self.params.dataframe[columns[0]],
                self.params.dataframe[columns[1]],
            )
        )
        result = self.params.dataframe[column].isin(valid_code_term_pairs[term_type])
        return result

    def _get_columns_by_meddra_variable_name(
        self,
    ) -> Optional[Tuple[str, Tuple[str, str]]]:
        """
        Extracts target name from params and
        returns associated term type and dataset columns.
        """
        variable_pair_map = {
            f"{self.params.domain}{MedDRAVariables.SOC.value}": (
                TermTypes.SOC.value,
                (
                    f"{self.params.domain}{MedDRAVariables.SOCCD.value}",
                    f"{self.params.domain}{MedDRAVariables.SOC.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.SOCCD.value}": (
                TermTypes.SOC.value,
                (
                    f"{self.params.domain}{MedDRAVariables.SOCCD.value}",
                    f"{self.params.domain}{MedDRAVariables.SOC.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.HLGT.value}": (
                TermTypes.HLGT.value,
                (
                    f"{self.params.domain}{MedDRAVariables.HLGTCD.value}",
                    f"{self.params.domain}{MedDRAVariables.HLGT.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.HLGTCD.value}": (
                TermTypes.HLGT.value,
                (
                    f"{self.params.domain}{MedDRAVariables.HLGTCD.value}",
                    f"{self.params.domain}{MedDRAVariables.HLGT.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.HLT.value}": (
                TermTypes.HLT.value,
                (
                    f"{self.params.domain}{MedDRAVariables.HLTCD.value}",
                    f"{self.params.domain}{MedDRAVariables.HLT.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.HLTCD.value}": (
                TermTypes.HLT.value,
                (
                    f"{self.params.domain}{MedDRAVariables.HLTCD.value}",
                    f"{self.params.domain}{MedDRAVariables.HLT.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.DECOD.value}": (
                TermTypes.PT.value,
                (
                    f"{self.params.domain}{MedDRAVariables.PTCD.value}",
                    f"{self.params.domain}{MedDRAVariables.DECOD.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.PTCD.value}": (
                TermTypes.PT.value,
                (
                    f"{self.params.domain}{MedDRAVariables.PTCD.value}",
                    f"{self.params.domain}{MedDRAVariables.DECOD.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.LLT.value}": (
                TermTypes.LLT.value,
                (
                    f"{self.params.domain}{MedDRAVariables.LLTCD.value}",
                    f"{self.params.domain}{MedDRAVariables.LLT.value}",
                ),
            ),
            f"{self.params.domain}{MedDRAVariables.LLTCD.value}": (
                TermTypes.LLT.value,
                (
                    f"{self.params.domain}{MedDRAVariables.LLTCD.value}",
                    f"{self.params.domain}{MedDRAVariables.LLT.value}",
                ),
            ),
        }
        return variable_pair_map.get(self.params.target)
