from cdisc_rules_engine.dataset_builders.base_dataset_builder import BaseDatasetBuilder
import pandas as pd


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
        variable_max_size (if needed by the rule)
        """
        # Get basic variable metadata
        variables_metadata = self.data_service.get_variables_metadata(
            dataset_name=self.dataset_path, datasets=self.datasets, drop_duplicates=True
        )

        # Check if the rule requires variable_max_size
        if (
            self.rule
            and self.rule.get("output_variables")
            and "variable_max_size" in self.rule["output_variables"]
        ):
            variables_metadata = self._add_variable_max_size(variables_metadata)

        return variables_metadata

    def _add_variable_max_size(self, variables_metadata):
        """
        Add variable_max_size column to the variables metadata.
        This column contains the maximum length of actual data for each variable.
        """
        # Get the dataset contents
        dataset = self.data_service.get_dataset(dataset_name=self.dataset_path)

        # Calculate max size for each variable
        max_sizes = {}
        for var_name in variables_metadata.data["variable_name"]:
            if var_name in dataset.data.columns:
                # Convert to string and get max length, ignoring null values
                max_length = dataset.data[var_name].dropna().astype(str).str.len().max()
                max_sizes[var_name] = max_length if not pd.isna(max_length) else 0
            else:
                max_sizes[var_name] = 0

        # Add the max_size column to metadata
        variables_metadata.data["variable_max_size"] = variables_metadata.data[
            "variable_name"
        ].map(max_sizes)

        return variables_metadata
