from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class ContentMetadataDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns the metadata from a given file as a dataframe with columns:
        dataset_size - File size
        dataset_location - Path to file
        dataset_name - Name of the dataset
        dataset_label - Label for the dataset
        """
        return self.build_split_datasets(self.dataset_path)

    def build_split_datasets(self, dataset_name, **kwargs):
        size_unit: str = self.rule_processor.get_size_unit_from_rule(self.rule)
        return self.data_service.get_dataset_metadata(
            dataset_name=dataset_name,
            size_unit=size_unit,
            datasets=self.datasets,
        )
