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
        rdomain_names_list = {}
        return Series(
            rdomain_names_list.setdefault(
                rdomain, self._get_parent_variable_names_list(rdomain)
            )
            for rdomain in self.params.dataframe.get("RDOMAIN", [])
        )

    def _get_parent_variable_names_list(self, rdomain: str):
        parent_datasets = [
            dataset["filename"]
            for dataset in self.params.datasets
            if dataset["domain"] == rdomain
        ]
        if len(parent_datasets) < 1:
            return []
        parent_dataframe = self.data_service.get_dataset(parent_datasets[0])
        return self._get_variable_names_list(rdomain, parent_dataframe)
