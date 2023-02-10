from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
import pandas as pd


class DomainListDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns a dataframe with a single row.
        The row contains a column for each domain and the value of that
        column is the domains file name

        dataset example:
           AE      EC
        0  ae.xpt  ec.xpt
        """

        return pd.DataFrame(
            {ds["domain"]: ds["filename"] for ds in self.datasets}, index=[0]
        )
