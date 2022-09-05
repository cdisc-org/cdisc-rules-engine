import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface


class Distinct(OperationInterface):
    def execute(self) -> pd.DataFrame:
        if not self.params.grouping:
            data = self.params.dataframe[self.params.target].unique()
            if isinstance(data[0], bytes):
                data = data.astype(str)
            data_converted_to_set = set(data)
            result = pd.Series([data_converted_to_set] * len(self.params.dataframe))
        else:
            grouped = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            )
            result = grouped[self.params.target].agg(
                lambda x: pd.Series([set(x.unique())])
            )
        return self._handle_operation_result(result)
