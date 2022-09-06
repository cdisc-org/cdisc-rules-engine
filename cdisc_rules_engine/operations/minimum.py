from cdisc_rules_engine.operations.base_operation import BaseOperation


class Minimum(BaseOperation):
    def _execute_operation(self):
        if not self.params.grouping:
            result = self.params.dataframe[self.params.target].min()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).min()
        return result
