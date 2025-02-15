from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.utilities.utils import (
    get_corresponding_datasets,
)


class ContentsDatasetBuilder(BaseDatasetBuilder):
    def build(self, **kwargs):
        """
        Returns the contents of a file as a dataframe for evaluation.
        """
        return self.data_service.get_dataset(dataset_name=self.dataset_path)

    def build_split_datasets(self, dataset_name, **kwargs):
        """
        Returns the contents of a file as a dataframe for evaluation.
        """
        return self.data_service.get_dataset(
            dataset_name=dataset_name, datasets=self.datasets
        )

    def get_dataset(self, **kwargs):
        dataset = super().get_dataset(**kwargs)
        length = sum(
            [
                dataset.record_count
                for dataset in get_corresponding_datasets(
                    self.datasets, self.dataset_metadata
                )
            ]
        )
        dataset.length = length
        return dataset
