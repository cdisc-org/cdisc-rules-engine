from cdisc_rules_engine.operations.base_operation import BaseOperation
from uuid import uuid4
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_terms_factory import (
    WhoDrugTermsFactory,
)
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_variable_names import (
    WhodrugVariableNames,
)


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
        valid_code_hierarchies = WhoDrugTermsFactory.get_code_hierarchies(terms)
        column = str(uuid4()) + "_codes"
        self.params.dataframe[column] = self.params.dataframe[code_strings].agg(
            "/".join, axis=1
        )
        result = self.params.dataframe[column].isin(valid_code_hierarchies)
        return result
