from cdisc_rules_engine.operations.library_model_column_order import (
    LibraryModelColumnOrder,
)
from pandas import Series
from collections import defaultdict


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
        domain_to_datasets = self._get_domain_to_datasets()
        rdomain_names_list = {}
        return Series(
            rdomain_names_list.setdefault(
                rdomain,
                self._get_parent_variable_names_list(domain_to_datasets, rdomain),
            )
            for rdomain in self.params.dataframe.get(
                "RDOMAIN", [None] * len(self.params.dataframe)
            )
        )

    def _get_domain_to_datasets(self):
        domain_to_datasets = defaultdict(list)
        for dataset in self.params.datasets:
            domain_to_datasets[dataset["domain"]].append(dataset)
        return domain_to_datasets

    def _get_parent_variable_names_list(self, domain_to_datasets: dict, rdomain: str):
        parent_datasets = domain_to_datasets.get(rdomain, [])
        if len(parent_datasets) < 1:
            return []
        parent_dataframe = self.data_service.get_dataset(parent_datasets[0]["filename"])
        return self._get_variable_names_list(rdomain, parent_dataframe)
