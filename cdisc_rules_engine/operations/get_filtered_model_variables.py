from typing import List

from cdisc_rules_engine.operations.base_operation import BaseOperation
from collections import OrderedDict


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

        # get variables metadata from the standard
        var_standard: List[dict] = self._get_variables_metadata_from_standard()
        # get subset of the selected variables
        var_standard_selected = [var for var in var_standard if var.get(key) == val]
        # replace variable names with domain abbreviation in them
        variable_names_list = self._replace_variable_wildcards(
            var_standard_selected, self.params.domain
        )
        # sort the list
        r1_var_standard = list(OrderedDict.fromkeys(variable_names_list))
        # get variables metadata from the model
        r2_var_model: List[dict] = self._get_variable_names_list(
            self.params.domain, self.params.dataframe
        )
        # get the common variables from standard model
        common_var_list = [
            element for element in r2_var_model if element in r1_var_standard
        ]
        return common_var_list
