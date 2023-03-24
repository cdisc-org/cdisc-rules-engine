from typing import List
from collections import OrderedDict
from cdisc_rules_engine.operations.library_model_column_order import (
    LibraryModelColumnOrder,
)
from pandas import Series


class ParentLibraryModelColumnOrder(LibraryModelColumnOrder):
    def _execute_operation(self):
        """
        Fetches column order for a supp's parent domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """
        if "RDOMAIN" not in self.params.dataframe:
            return []
        rdomains = self.params.dataframe["RDOMAIN"].unique()
        rdomain_names_list = {}
        for rdomain in rdomains:
            parent_datasets = [
                dataset["filename"]
                for dataset in self.params.datasets
                if dataset["domain"] == rdomain
            ]
            if len(parent_datasets) < 1:
                rdomain_names_list[rdomain] = []
            else:
                parent_dataframe = self.data_service.get_dataset(parent_datasets[0])
                # get variables metadata from the standard model
                variables_metadata: List[
                    dict
                ] = self._get_variables_metadata_from_standard_model(
                    rdomain, parent_dataframe
                )
                # create a list of variable names in accordance to the "ordinal" key
                variable_names_list = self._replace_variable_wildcards(
                    variables_metadata, rdomain
                )
                rdomain_names_list[rdomain] = list(
                    OrderedDict.fromkeys(variable_names_list)
                )
        return Series(
            rdomain_names_list[rdomain] for rdomain in self.params.dataframe["RDOMAIN"]
        )
