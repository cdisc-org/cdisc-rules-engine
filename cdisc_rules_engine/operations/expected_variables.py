from cdisc_rules_engine.constants.permissibility import (
    EXPECTED,
    PERMISSIBILITY_KEY,
)
from cdisc_rules_engine.operations.library_column_order import LibraryColumnOrder


class ExpectedVariables(LibraryColumnOrder):
    def _execute_operation(self):
        """
        Fetches required variables for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """
        self.params.key_name = PERMISSIBILITY_KEY
        self.params.key_value = EXPECTED
        return super()._execute_operation()
