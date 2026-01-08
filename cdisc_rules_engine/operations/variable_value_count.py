from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.operations.base_operation import BaseOperation
import asyncio
import os
from collections import Counter
from typing import List
from cdisc_rules_engine.utilities.utils import (
    get_corresponding_datasets,
    tag_source,
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
            {dataset.domain: dataset for dataset in self.params.datasets}.values()
        )
        coroutines = [
            self._get_dataset_variable_value_count(dataset)
            for dataset in datasets_with_unique_domains
        ]
        dataset_variable_value_counts: List[Counter] = await asyncio.gather(*coroutines)
        return dict(sum(dataset_variable_value_counts, Counter()))

    async def _get_dataset_variable_value_count(
        self, dataset_metadata: SDTMDatasetMetadata
    ) -> Counter:
        if dataset_metadata.is_split:
            corresponding_datasets = get_corresponding_datasets(
                self.params.datasets, dataset_metadata
            )
            data: DatasetInterface = self.data_service.concat_split_datasets(
                self.data_service.get_dataset, corresponding_datasets
            )
        else:
            data: DatasetInterface = self.data_service.get_dataset(
                dataset_name=os.path.join(
                    self.params.directory_path, dataset_metadata.filename
                )
            )
            data = tag_source(data, dataset_metadata)
        target_variable = self.params.original_target.replace(
            "--", dataset_metadata.domain, 1
        )
        if target_variable in data:
            return Counter(data[target_variable].unique())
        else:
            return Counter()
