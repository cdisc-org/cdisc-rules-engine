from cdisc_rules_engine.operations.base_operation import BaseOperation


class VariableExists(BaseOperation):
    def _execute_operation(self):
        # get metadata
        return self.params.target in self.params.dataframe
