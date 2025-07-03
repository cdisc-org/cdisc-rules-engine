from typing import List
from cdisc_rules_engine.operations.base_operation import BaseOperation


class LibraryModelVariablesFilter(BaseOperation):
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
        """

        return self._get_model_filtered_variables()

    def _get_model_filtered_variables(self):
        key = self.params.key_name
        val = self.params.key_value
        model_variables: List[dict] = self._get_variables_metadata_from_standard_model(
            self.params.domain, self.params.dataframe
        )
        filtered_model = [var for var in model_variables if var.get(key) == val]
        variable_names_list = self._replace_variable_wildcards(
            filtered_model, self.params.domain
        )
        return variable_names_list
