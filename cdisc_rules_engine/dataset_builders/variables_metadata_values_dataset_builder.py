from cdisc_rules_engine.dataset_builders.values_dataset_builder import (
    ValuesDatasetBuilder,
)
from typing import Any


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
        # Get dataset contents and convert it from wide to long
        data_contents_long_df = super().build(self)
        # Get variable metadata
        variable_metadata = self.data_service.get_variables_metadata(
            dataset_name=self.dataset_path, datasets=self.datasets, drop_duplicates=True
        )

        # Merge with the long dataset
        merged_df = data_contents_long_df.merge(
            variable_metadata, how="left", on="variable_name"
        )
        # Calculate value length
        merged_df["variable_value_length"] = merged_df.apply(
            lambda row: self.calculate_variable_value_length(
                row["variable_value"], row["variable_data_type"]
            ),
            axis=1,
        )
        return merged_df

    def build_split_datasets(self, dataset_name, **kwargs):
        """Handle split datasets by applying the same logic to each subset."""
        # Get dataset contents and convert it from wide to long
        data_contents_long_df = super().build_split_datasets(dataset_name, **kwargs)

        # Get variable metadata
        variable_metadata = self.data_service.get_variables_metadata(
            dataset_name=dataset_name, datasets=self.datasets, drop_duplicates=True
        )

        # Merge with the long dataset
        merged_df = data_contents_long_df.merge(
            variable_metadata, how="left", on="variable_name"
        )

        # Calculate value length
        merged_df["variable_value_length"] = merged_df.apply(
            lambda row: self.calculate_variable_value_length(
                row["variable_value"], row["variable_data_type"]
            ),
            axis=1,
        )

        return merged_df

    def calculate_variable_value_length(self, value: Any, data_type: str) -> int:
        """Calculate the length of a variable value based on its data type."""
        if data_type and data_type.lower() == "char":
            return len(str(value)) if value is not None else 0
        else:
            return 0  # For numeric values, length isn't relevant for this rule
