from cdisc_rules_engine.operations.base_operation import BaseOperation
from typing import List


class LibraryColumnOrder(BaseOperation):
    def _execute_operation(self):
        """
        Fetches column order for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.

        If key_name and key_value are provided, filter variables based on specified criteria.

        Optional parameters:
        - key_name: The metadata key to filter by (e.g., "role", "type", etc.)
        - key_value: The value to match for the filter key (e.g., "Timing", "Identifier", etc.)
        """
        # Get variables metadata from the standard model for the current domain
        variables_metadata: List[dict] = self._get_variables_metadata_from_standard()

        # Filter variables based on the specified criteria

        if self.params.key_name:
            variables_metadata = [
                var
                for var in variables_metadata
                if var.get(self.params.key_name) == self.params.key_value
            ]

        # Replace variable wildcards with actual domain names
        variable_names_list = self._replace_variable_wildcards(
            variables_metadata, self.params.domain
        )

        return variable_names_list
