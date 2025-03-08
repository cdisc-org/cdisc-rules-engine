import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
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
            {dataset.unsplit_name: dataset for dataset in self.params.datasets}.values()
        )
        coroutines = [
            self._get_dataset_variable_count(dataset)
            for dataset in datasets_with_unique_domains
        ]
        dataset_variable_value_counts: List[int] = await asyncio.gather(*coroutines)
        return sum(dataset_variable_value_counts)

    async def _get_dataset_variable_count(
        self, dataset: SDTMDatasetMetadata
    ) -> Counter:
        data: pd.DataFrame = self.data_service.get_dataset(
            dataset_name=dataset.full_path
        )
        target_variable = (
            self.params.original_target.replace("--", dataset.domain, 1)
            if dataset.domain
            else self.params.original_target
        )
        return 1 if target_variable in data else 0
