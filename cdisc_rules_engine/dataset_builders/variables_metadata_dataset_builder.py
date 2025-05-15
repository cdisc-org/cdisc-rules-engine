from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder


class VariablesMetadataDatasetBuilder(BaseDatasetBuilder):
    def build(self):
        """
         Returns the variable metadata from a given file as a dataframe.
         The resulting dataframe has the following columns:
        variable_name
        variable_order_number
        variable_label
        variable_size
        variable_data_type
        variable_format
        """
        return self.data_service.get_variables_metadata(
            dataset_name=self.dataset_path, datasets=self.datasets, drop_duplicates=True
        )
