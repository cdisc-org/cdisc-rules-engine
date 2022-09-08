from cdisc_rules_engine.operations.base_operation import BaseOperation
from uuid import uuid4
from cdisc_rules_engine.models.dictionaries.meddra.meddra_variables import (
    MedDRAVariables,
)
from cdisc_rules_engine.models.dictionaries.meddra.terms.meddra_term import MedDRATerm
from cdisc_rules_engine.models.dictionaries.meddra.terms.term_types import TermTypes
from cdisc_rules_engine.utilities.utils import get_meddra_code_term_pairs_cache_key
from typing import Optional, Tuple


class MedDRACodeTermPairsValidator(BaseOperation):
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
        soccd_column = f"{self.params.domain}{MedDRAVariables.SOCCD.value}"
        soc_column = f"{self.params.domain}{MedDRAVariables.SOC.value}"
        hlgtcd_column = f"{self.params.domain}{MedDRAVariables.HLGTCD.value}"
        hlgt_column = f"{self.params.domain}{MedDRAVariables.HLGT.value}"
        hltcd_column = f"{self.params.domain}{MedDRAVariables.HLTCD.value}"
        hlt_column = f"{self.params.domain}{MedDRAVariables.HLT.value}"
        ptcd_column = f"{self.params.domain}{MedDRAVariables.PTCD.value}"
        decod_column = f"{self.params.domain}{MedDRAVariables.DECOD.value}"
        llt_column = f"{self.params.domain}{MedDRAVariables.LLT.value}"
        lltcd_column = f"{self.params.domain}{MedDRAVariables.LLTCD.value}"

        variable_pair_map = {
            soc_column: (
                TermTypes.SOC.value,
                (
                    soccd_column,
                    soc_column,
                ),
            ),
            soccd_column: (
                TermTypes.SOC.value,
                (
                    soccd_column,
                    soc_column,
                ),
            ),
            hlgt_column: (
                TermTypes.HLGT.value,
                (
                    hlgtcd_column,
                    hlgt_column,
                ),
            ),
            hlgtcd_column: (
                TermTypes.HLGT.value,
                (
                    hlgtcd_column,
                    hlgt_column,
                ),
            ),
            hlt_column: (
                TermTypes.HLT.value,
                (
                    hltcd_column,
                    hlt_column,
                ),
            ),
            hltcd_column: (
                TermTypes.HLT.value,
                (
                    hltcd_column,
                    hlt_column,
                ),
            ),
            decod_column: (
                TermTypes.PT.value,
                (
                    ptcd_column,
                    decod_column,
                ),
            ),
            ptcd_column: (
                TermTypes.PT.value,
                (
                    ptcd_column,
                    decod_column,
                ),
            ),
            llt_column: (
                TermTypes.LLT.value,
                (
                    lltcd_column,
                    llt_column,
                ),
            ),
            lltcd_column: (
                TermTypes.LLT.value,
                (
                    lltcd_column,
                    llt_column,
                ),
            ),
        }
        return variable_pair_map.get(self.params.target)
