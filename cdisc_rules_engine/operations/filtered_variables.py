from typing import List
from cdisc_rules_engine.operations.base_operation import BaseOperation


class FilteredVariables(BaseOperation):
    def _execute_operation(self):
        """
        Filter variables from the library based on specified criteria.

        Expected parameters:
        - key_name: The metadata key to filter by (e.g., "role", "type", etc.)
        - key_value: The value to match for the filter key (e.g., "Timing", "Identifier", etc.)
        """
        filter_key = self.params.key_name
        filter_value = self.params.key_value

        # Get variables metadata from the standard model for the current domain
        variables_metadata: List[dict] = self._get_variables_metadata_from_standard()

        # Filter variables based on the specified criteria
        filtered_variables = [
            var for var in variables_metadata if var.get(filter_key) == filter_value
        ]

        # Replace variable wildcards with actual domain names
        variable_names_list = self._replace_variable_wildcards(
            filtered_variables, self.params.domain
        )

        return variable_names_list
