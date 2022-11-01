from cdisc_rules_engine.operations.base_operation import BaseOperation
from uuid import uuid4
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_variable_names import (
    WhodrugVariableNames,
)
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_record_types import (
    WhodrugRecordTypes,
)
from typing import Set


class WhodrugHierarchyValidator(BaseOperation):
    def _execute_operation(self):
        # get metadata
        if not self.params.whodrug_path:
            raise ValueError("Can't execute the operation, no whodrug path provided")

        terms: dict = self.cache.get(self.params.whodrug_path)
        code_variables = [
            WhodrugVariableNames.DRUG_NAME.value,
            WhodrugVariableNames.ATC_TEXT.value,
            WhodrugVariableNames.ATC_CLASSIFICATION.value,
        ]
        code_strings = [
            f"{self.params.domain}{variable}" for variable in code_variables
        ]
        valid_code_hierarchies = self.get_code_hierarchies(terms)
        column = str(uuid4()) + "_codes"
        self.params.dataframe[column] = self.params.dataframe[code_strings].agg(
            "/".join, axis=1
        )
        result = self.params.dataframe[column].isin(valid_code_hierarchies)
        return result

    def get_code_hierarchies(self, term_map: dict) -> Set[str]:
        valid_codes = set()
        for atc_class in term_map.get(
            WhodrugRecordTypes.ATC_CLASSIFICATION.value, {}
        ).values():
            atc_text = term_map.get(WhodrugRecordTypes.ATC_TEXT.value, {}).get(
                atc_class.code
            )
            if atc_text:
                drug_dict = term_map.get(WhodrugRecordTypes.DRUG_DICT.value, {}).get(
                    atc_class.get_parent_identifier()
                )
                if drug_dict:
                    valid_codes.add(
                        f"{drug_dict.drugName}/{atc_text.text}/{atc_class.code}"
                    )

        return valid_codes
