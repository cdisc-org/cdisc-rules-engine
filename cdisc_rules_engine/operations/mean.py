import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface


class Mean(OperationInterface):
    def execute(self) -> pd.DataFrame:
        if not self.params.grouping:
            result = self.params.dataframe[self.params.target].mean()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).mean()
        return self._handle_operation_result(result)
