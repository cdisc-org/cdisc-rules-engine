import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface


class Minimum(OperationInterface):
    def execute(self) -> pd.DataFrame:
        if not self.params.grouping:
            result = self.params.dataframe[self.params.target].min()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).min()
        return self._handle_operation_result(result)
