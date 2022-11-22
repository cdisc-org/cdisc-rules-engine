from cdisc_rules_engine.operations.base_operation import BaseOperation
from typing import Generator
from cdisc_rules_engine.models.dictionaries.whodrug.whodrug_record_types import (
    WhodrugRecordTypes,
)


class WhodrugReferencesValidator(BaseOperation):
    def _execute_operation(self):
        # get metadata
        """
        Checks if a reference to whodrug term points
        to the existing code in Atc Text (INA) file.
        """
        if not self.params.whodrug_path:
            raise ValueError("Can't execute the operation, no whodrug path provided")

        terms: dict = self.cache.get(self.params.whodrug_path)
        valid_codes: Generator = (
            term.code for term in terms[WhodrugRecordTypes.ATC_TEXT.value].values()
        )
        result = self.params.dataframe[self.params.target].isin(valid_codes)
        return result
