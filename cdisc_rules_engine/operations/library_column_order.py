from cdisc_rules_engine.operations.base_operation import BaseOperation
from typing import List
from collections import OrderedDict


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
        """

        # get variables metadata from the standard model
        variables_metadata: List[dict] = self._get_variables_metadata_from_standard()

        # create a list of variable names in accordance to the "ordinal" key
        variable_names_list = [
            var["name"].replace("--", self.params.domain) for var in variables_metadata
        ]
        return list(OrderedDict.fromkeys(variable_names_list))
