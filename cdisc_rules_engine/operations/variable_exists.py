import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface


class VariableExists(OperationInterface):
    def execute(self) -> pd.DataFrame:
        # get metadata
        return self._handle_operation_result(
            self.params.target in self.params.dataframe
        )
