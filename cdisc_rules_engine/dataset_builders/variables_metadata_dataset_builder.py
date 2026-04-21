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
        if self.rule and self._needs_variable_max_size():
            variables_metadata = self._add_variable_max_size(variables_metadata)

        return variables_metadata

    def _needs_variable_max_size(self):
        """
        Check if the rule requires variable_max_size by examining:
        - output_variables
        - operations (operator field)
        - conditions (all fields)
        """
        return (
            self._check_output_variables_for_variable_max_size()
            or self._check_operations_for_variable_max_size()
            or self._check_conditions_for_variable_max_size(self.rule.get("conditions"))
        )

    def _check_output_variables_for_variable_max_size(self):
        """Check if output_variables contains variable_max_size."""
        output_variables = self.rule.get("output_variables")
        if not output_variables:
            return False

        if isinstance(output_variables, list):
            return "variable_max_size" in output_variables
        elif isinstance(output_variables, dict):
            return "variable_max_size" in output_variables.values()

        return False

    def _check_operations_for_variable_max_size(self):
        """Check if operations contains variable_max_size operator."""
        operations = self.rule.get("operations")
        if not operations:
            return False

        if isinstance(operations, list):
            return any(
                isinstance(op, dict) and op.get("operator") == "variable_max_size"
                for op in operations
            )
        elif isinstance(operations, dict):
            return operations.get("operator") == "variable_max_size"

        return False

    def _check_conditions_for_variable_max_size(self, conditions):
        """
        Recursively check conditions for variable_max_size references.
        Handles both ConditionComposite objects and dict/list structures.
        """
        # If it's a ConditionComposite object, use its methods
        if hasattr(conditions, "values"):
            # Get all condition values as a flat list of dicts
            condition_values = conditions.values()
            for condition_dict in condition_values:
                if self._contains_variable_max_size(condition_dict):
                    return True
        # If it's a dict, check recursively
        elif isinstance(conditions, dict):
            if self._contains_variable_max_size(conditions):
                return True
        # If it's a list, check each item
        elif isinstance(conditions, list):
            for item in conditions:
                if self._check_conditions_for_variable_max_size(item):
                    return True

        return False

    def _contains_variable_max_size(self, data):
        """
        Check if data contains 'variable_max_size' reference.
        Handles strings, dictionaries, and lists.
        """
        if data == "variable_max_size":
            return True
        elif isinstance(data, dict):
            for key, value in data.items():
                if value == "variable_max_size":
                    return True
                # Recursively check nested structures
                if isinstance(value, (dict, list)):
                    if self._check_conditions_for_variable_max_size(value):
                        return True
        elif isinstance(data, list):
            for item in data:
                if self._contains_variable_max_size(item):
                    return True

        return False

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
