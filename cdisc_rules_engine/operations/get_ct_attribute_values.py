from typing import List

from cdisc_rules_engine.operations.base_operation import BaseOperation
from collections import OrderedDict


class CTAttributeValues(BaseOperation):
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

        return self._get_ct_attribute_values()

    def _get_ct_attribute_values(self):
        key = self.params.key_name
        val = self.params.key_value

        # get variables metadata from the standard model
        var_metadata: List[dict] = self._get_variables_metadata_from_standard()

        variables_metadata = [var for var in var_metadata if var.get(key) == val]

        # create a list of variable names in accordance to the "ordinal" key
        variable_names_list = self._replace_variable_wildcards(
            variables_metadata, self.params.domain
        )

        r_list = list(OrderedDict.fromkeys(variable_names_list))
        return r_list
