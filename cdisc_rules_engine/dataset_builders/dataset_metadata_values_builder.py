from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)


class ValueCheckDatasetMetadataDatasetBuilder(ValuesDatasetBuilder):
    def build(self):
        """
        Returns a long dataset where each value in each row of the original dataset is
        a row in the new dataset with dataset metadata attached.
        Columns available in the dataset include:
        - "row_number"
        - "variable_name"
        - "variable_value"
        - dataset_size - File size
        - dataset_location - Path to file
        - dataset_name - Name of the dataset
        - dataset_label - Label for the dataset
        - is_ap - Whether the domain is an AP domain
        - ap_suffix - The 2-character suffix from AP domains
        """
        size_unit: str = self.rule_processor.get_size_unit_from_rule(self.rule)
        dataset_metadata = self.data_service.get_dataset_metadata(
            dataset_name=self.dataset_path,
            size_unit=size_unit,
            datasets=self.datasets,
        )
        dataset_metadata = dataset_metadata.to_dict(orient="records")[0]
        data_contents_long_df = super().build()
        row_count = len(data_contents_long_df)
        for key, value in dataset_metadata.items():
            data_contents_long_df[key] = [value] * row_count
        return data_contents_long_df
