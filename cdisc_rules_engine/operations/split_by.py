from cdisc_rules_engine.operations.base_operation import BaseOperation


class SplitBy(BaseOperation):
    def _execute_operation(self):
        return self.params.dataframe[self.params.target].str.split(
            self.params.delimiter
        )
