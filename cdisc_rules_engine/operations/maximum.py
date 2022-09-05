import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface


class Maximum(OperationInterface):
    def execute(self) -> pd.DataFrame:
        if not self.params.grouping:
            result = self.params.dataframe[self.params.target].max()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).max()
        return self._handle_operation_result(result)
