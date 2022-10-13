import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
import asyncio
from collections import Counter
from typing import List, Set


class VariableCount(BaseOperation):
    """
    Counts the number of times the value of the target
    column appears as a variable in the study.
    """

    def _execute_operation(self):
        # get metadata
        variable_count = asyncio.run(self._get_all_study_variable_counts())
        return variable_count

    async def _get_all_study_variable_counts(self) -> dict:
        """
        Returns a mapping of target values to the number
        of times that value appears as a variable in the study.
        """
        datasets_with_unique_domains = list(
            {dataset["domain"]: dataset for dataset in self.params.datasets}.values()
        )
        target_variable = self.params.target.replace("--", self.params.domain, 1)
        values = set(self.params.dataframe[target_variable].unique())
        coroutines = [
            self._get_dataset_variable_count(dataset, values)
            for dataset in datasets_with_unique_domains
        ]
        dataset_variable_value_counts: List[Counter] = await asyncio.gather(*coroutines)
        counts = dict(sum(dataset_variable_value_counts, Counter()))
        return self.params.dataframe[target_variable].map(counts)

    async def _get_dataset_variable_count(
        self, dataset: dict, values: Set[str]
    ) -> Counter:
        data: pd.DataFrame = self.data_service.get_dataset(
            f"{self.params.directory_path}/{dataset.get('filename')}"
        )
        column_value_intersection = values and set(data.columns)
        return Counter(column_value_intersection)
