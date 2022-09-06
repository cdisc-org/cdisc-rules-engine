import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
import asyncio
from collections import Counter
from typing import List
from cdisc_rules_engine.utilities.utils import (
    get_corresponding_datasets,
    is_split_dataset,
)


class VariableValueCount(BaseOperation):
    def _execute_operation(self):
        # get metadata
        variable_value_count = asyncio.run(self._get_all_study_variable_value_counts())
        return variable_value_count

    async def _get_all_study_variable_value_counts(self) -> dict:
        """
        Returns a mapping of variable values to the number
        of times that value appears in the study.
        """
        datasets_with_unique_domains = list(
            {dataset["domain"]: dataset for dataset in self.params.datasets}.values()
        )
        coroutines = [
            self._get_dataset_variable_value_count(dataset)
            for dataset in datasets_with_unique_domains
        ]
        dataset_variable_value_counts: List[Counter] = await asyncio.gather(*coroutines)
        return dict(sum(dataset_variable_value_counts, Counter()))

    async def _get_dataset_variable_value_count(self, dataset: dict) -> Counter:
        domain = dataset.get("domain")
        if is_split_dataset(self.params.datasets, domain):
            files = [
                f"{self.params.directory_path}/{dataset.get('filename')}"
                for dataset in get_corresponding_datasets(self.params.datasets, domain)
            ]
            data: pd.DataFrame = self.data_service.join_split_datasets(
                self.data_service.get_dataset, files
            )
        else:
            data: pd.DataFrame = self.data_service.get_dataset(
                f"{self.params.directory_path}/{dataset.get('filename')}"
            )
        target_variable = self.params.target.replace("--", domain, 1)
        if target_variable in data:
            return Counter(data[target_variable].unique())
        else:
            return Counter()
