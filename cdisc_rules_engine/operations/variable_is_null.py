from cdisc_rules_engine.operations.base_operation import BaseOperation
import pandas as pd


class VariableIsNull(BaseOperation):
    def _execute_operation(self):
        # Always get the content dataframe. Similar to variable_exists check
        dataframe = self.data_service.get_dataset(self.params.dataset_path)
        if self.params.target.startswith("define_variable"):
            # Handle checks against define metadata
            target_column = self.evaluation_dataset[self.params.target]
            result = [
                self._is_target_variable_null(dataframe, value)
                for value in target_column
            ]
            return pd.Series(result)
        else:
            target_variable = self.params.target.replace("--", self.params.domain, 1)
            return self._is_target_variable_null(dataframe, target_variable)

    def _is_target_variable_null(
        self, dataframe: pd.DataFrame, target_variable: str
    ) -> bool:
        if target_variable not in dataframe:
            return True
        series = dataframe[target_variable]
        return series.mask(series == "").isnull().all()
