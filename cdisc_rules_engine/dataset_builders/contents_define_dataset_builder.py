from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class ContentsDefineDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
        Returns the original dataset along with additional columns
        for dataset metadata and the define xml dataset metadata.
        Columns available in the dataset are
        the columns in the original dataset along with
        the following columns from Define XML:

        dataset_label - Label for the dataset
        dataset_location - Path to file
        dataset_name - Name of the dataset
        dataset_size - File size
        dataset_domain - Domain of the dataset
        define_dataset_class - dataset class
        define_dataset_domain - dataset domain from define
        define_dataset_is_non_standard - whether a dataset is a standard
        define_dataset_key_sequence - ordered list of key sequence variables in the dataset (and supp)
        define_dataset_label - dataset label from define
        define_dataset_location - dataset location from define
        define_dataset_name - dataset name from define_xml
        define_dataset_structure - dataset structure
        define_dataset_variables - list of variables in the dataset

        ...,
        """
        data_contents_df = self.data_service.get_dataset(
            dataset_name=self.dataset_path, datasets=self.datasets
        )
        # Build dataset metadata dataframe
        size_unit: str = self.rule_processor.get_size_unit_from_rule(self.rule)
        dataset_metadata = self.data_service.get_dataset_metadata(
            dataset_name=self.dataset_path, size_unit=size_unit, datasets=self.datasets
        ).to_dict(orient="records")[0]
        # Build define xml dataframe
        define = self.get_define_xml_item_group_metadata_for_dataset(dataset_metadata)
        # Horizontally concat the data frames
        filled = self.dataset_implementation.from_records(
            [dataset_metadata | define] * data_contents_df.length
        )
        concat = data_contents_df.concat(filled, axis=1)
        return concat
