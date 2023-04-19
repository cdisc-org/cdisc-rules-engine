from cdisc_rules_engine.operations.base_operation import BaseOperation


class VariableExists(BaseOperation):
    def _execute_operation(self):
        # get metadata
        dataframe = self.data_service.get_dataset(self.params.dataset_path)
        return self.params.target in dataframe
