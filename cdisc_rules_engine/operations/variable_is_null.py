from cdisc_rules_engine.operations.base_operation import BaseOperation


class VariableIsNull(BaseOperation):
    def _execute_operation(self):
        target_variable = self.params.target.replace("--", self.params.domain, 1)
        if target_variable not in self.params.dataframe:
            return True
        series = self.params.dataframe[target_variable]
        return series.mask(series == "").isnull().all()
