from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.utilities.utils import (
    is_split_dataset,
    get_corresponding_datasets,
)


class ContentsDatasetBuilder(BaseDatasetBuilder):
    def build(self, **kwargs):
        """
        Returns the contents of a file as a dataframe for evaluation.
        """
        return self.data_service.get_dataset(dataset_name=self.dataset_path)

    def build_split_dataset(self, dataset_name, **kwargs):
        """
        Returns the contents of a file as a dataframe for evaluation.
        """
        return self.data_service.get_dataset(
            dataset_name=dataset_name, datasets=self.datasets
        )

    def get_dataset(self, **kwargs):
        # If validating dataset content, ensure split datasets are handled.
        if is_split_dataset(self.datasets, self.domain):
            # Handle split datasets for content checks.
            # A content check is any check that is not in the list of rule types
            dataset = self.data_service.concat_split_datasets(
                func_to_call=self.build_split_dataset,
                dataset_names=self.get_corresponding_datasets_names(),
                **kwargs,
            )
        else:
            # single dataset. the most common case
            dataset = self.build(**kwargs)
        length = sum(
            [
                dataset.get("length", 0)
                for dataset in get_corresponding_datasets(self.datasets, self.domain)
            ]
        )
        dataset.length = length
        return dataset
