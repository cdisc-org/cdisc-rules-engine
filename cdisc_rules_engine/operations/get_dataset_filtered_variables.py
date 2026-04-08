from cdisc_rules_engine.operations.library_column_order import LibraryColumnOrder


class GetDatasetFilteredVariables(LibraryColumnOrder):
    def _execute_operation(self):
        """
        Filter variables from the dataset based on specified criteria.

        Expected parameters:
        - key_name: The metadata key to filter by (e.g., "role", "type", etc.)
        - key_value: The value to match for the filter key (e.g., "Timing", "Identifier", etc.)
        """
        variable_names_list = super()._execute_operation()

        # Get actual column names from the dataset that match our filtered list
        dataset_columns = self.params.dataframe.columns.tolist()
        matched_variables = [
            var_name for var_name in variable_names_list if var_name in dataset_columns
        ]

        return matched_variables
