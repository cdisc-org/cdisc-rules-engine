from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.operations.base_operation import BaseOperation
import asyncio
import os
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
                os.path.join(self.params.directory_path, dataset.get("filename"))
                for dataset in get_corresponding_datasets(self.params.datasets, domain)
            ]
            data: DatasetInterface = self.data_service.concat_split_datasets(
                self.data_service.get_dataset, files
            )
        else:
            data: DatasetInterface = self.data_service.get_dataset(
                dataset_name=os.path.join(
                    self.params.directory_path, dataset.get("filename")
                )
            )
        target_variable = self.params.original_target.replace("--", domain, 1)
        if target_variable in data:
            return Counter(data[target_variable].unique())
        else:
            return Counter()
