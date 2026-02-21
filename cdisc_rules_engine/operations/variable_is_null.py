from cdisc_rules_engine.operations.base_operation import BaseOperation


class VariableIsNull(BaseOperation):
    def _execute_operation(self):
        if self.params.source == "submission":
            if self.params.level == "row":
                raise ValueError("level: row may only be used with source: evaluation")
            dataframe = self.data_service.get_dataset(
                dataset_name=self.params.dataset_path
            )
        else:
            dataframe = self.evaluation_dataset

        return self._is_target_variable_null(dataframe, self.params.target)

    def _is_target_variable_null(self, dataframe, target_variable: str) -> bool:
        if target_variable not in dataframe:
            return True
        series = dataframe[target_variable]
        return (series.isnull() | (series == "")).all()
