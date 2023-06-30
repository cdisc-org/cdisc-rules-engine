from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class ContentsDefineDatasetBuilder(BaseDatasetBuilder):
    def build(self, **kwargs):
        """
        Returns a long dataset where each value in each row of the original dataset is
        a row in the new dataset.
        The define xml variable metadata corresponding to each row's variable is
        attached to each row.
        Columns available in the dataset are:

        dataset_size - File size
        dataset_location - Path to file
        dataset_name - Name of the dataset
        dataset_label - Label for the dataset

        Columns from Define XML:
        define_dataset_name - dataset name from define_xml
        define_dataset_label - dataset label from define
        define_dataset_location - dataset location from define
        define_dataset_class - dataset class
        define_dataset_structure - dataset structure
        define_dataset_is_non_standard - whether a dataset is a standard
        define_dataset_variables - dataset variable list

        ...,
        """
        return self.get_dataset_define_metadata(**kwargs)
