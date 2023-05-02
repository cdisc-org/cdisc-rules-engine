from cdisc_rules_engine.operations.base_operation import BaseOperation


class VariableIsNull(BaseOperation):
    def _execute_operation(self):
        # Always get the content dataframe. Similar to variable_exists check
        dataframe = self.data_service.get_dataset(self.params.dataset_path)
        target_variable = self.params.target.replace("--", self.params.domain, 1)
        if target_variable not in dataframe:
            return True
        series = dataframe[target_variable]
        return series.mask(series == "").isnull().all()
