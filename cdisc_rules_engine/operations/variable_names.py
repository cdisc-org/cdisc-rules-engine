from cdisc_rules_engine.operations.base_operation import BaseOperation


class VariableNames(BaseOperation):
    def _execute_operation(self):
        """
        Return the set of variable names for the given standard
        """
        variable_details = self.library_metadata.variables_metadata
        all_variables = [
            list(variable_details[dataset].values()) for dataset in variable_details
        ]

        return set(
            [variable["name"] for variables in all_variables for variable in variables]
        )
