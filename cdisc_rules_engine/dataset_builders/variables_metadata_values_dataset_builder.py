from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)


class ValueCheckVariableMetadataDatasetBuilder(ValuesDatasetBuilder):
    def build(self):
        """
        Returns a long dataset where each value in each row of the original dataset is
        a row in the new dataset, with variable metadata attached.

        Columns available in the dataset include:
        - "row_number"
        - "variable_name"
        - "variable_value"
        - "variable_order_number"
        - "variable_label"
        - "variable_size"
        - "variable_data_type"
        - "variable_format"
        - "variable_value_length"
        """
        data_contents_long_df = super().build()
        variable_metadata = self.data_service.get_variables_metadata(
            dataset_name=self.dataset_path, datasets=self.datasets, drop_duplicates=True
        )
        merged_df = data_contents_long_df.merge(
            variable_metadata, how="left", on="variable_name"
        )
        merged_df["variable_value_length"] = merged_df.apply(
            lambda row: ValuesDatasetBuilder.calculate_variable_value_length(
                row["variable_value"], row["variable_data_type"]
            ),
            axis=1,
        )
        return merged_df
