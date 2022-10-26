import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
import asyncio
from collections import Counter
from typing import List


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
        coroutines = [
            self._get_dataset_variable_count(dataset)
            for dataset in datasets_with_unique_domains
        ]
        dataset_variable_value_counts: List[int] = await asyncio.gather(*coroutines)
        return sum(dataset_variable_value_counts)

    async def _get_dataset_variable_count(self, dataset: dict) -> Counter:
        domain = dataset.get("domain", "")
        data: pd.DataFrame = self.data_service.get_dataset(
            f"{self.params.directory_path}/{dataset.get('filename')}"
        )
        target_variable = self.params.target.replace("--", domain, 1)
        return 1 if target_variable in data else 0
