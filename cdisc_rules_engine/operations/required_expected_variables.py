from cdisc_rules_engine.constants.permissibility import (
    EXPECTED,
    PERMISSIBILITY_KEY,
    REQUIRED,
)
from cdisc_rules_engine.operations.library_column_order import LibraryColumnOrder


class RequiredExpectedVariables(LibraryColumnOrder):
    def _execute_operation(self):
        """
        Returns variables whose Core is Req or Exp, preserving IG ordinal order.
        """
        variables_metadata = self._get_variables_metadata_from_standard()

        variables_metadata = [
            variable
            for variable in variables_metadata
            if variable.get(PERMISSIBILITY_KEY) in {REQUIRED, EXPECTED}
        ]
        return self._replace_variable_wildcards(variables_metadata, self.params.domain)
