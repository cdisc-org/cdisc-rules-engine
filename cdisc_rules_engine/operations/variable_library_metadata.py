from cdisc_rules_engine.operations.base_operation import BaseOperation


class VariableLibraryMetadata(BaseOperation):
    def _execute_operation(self):
        """
        Get the variable permissibility values for all data in the current
        dataset.
        """
        variable_details: dict = self.library_metadata.variables_metadata
        dataset_variable_details = variable_details.get(self.params.domain, {})
        variable_metadata = {
            key: dataset_variable_details[key].get(self.params.target)
            for key in dataset_variable_details.keys()
        }
        return variable_metadata
