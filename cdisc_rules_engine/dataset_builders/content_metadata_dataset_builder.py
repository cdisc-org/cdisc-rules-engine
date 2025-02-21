from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
from cdisc_rules_engine.utilities.utils import tag_source


class ContentMetadataDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns the metadata from a given file as a dataframe with columns:
        dataset_size - File size
        dataset_location - Path to file
        dataset_name - Name of the dataset
        dataset_label - Label for the dataset
        """
        size_unit: str = self.rule_processor.get_size_unit_from_rule(self.rule)
        return self.data_service.get_dataset_metadata(
            dataset_name=self.dataset_path, size_unit=size_unit
        )

    def get_dataset(self):
        """
        Merging split datasets is irrelevant for getting dataset metadata,
          so override base
        """
        dataset = self.build()
        dataset = tag_source(dataset, self.dataset_metadata)
        return dataset
