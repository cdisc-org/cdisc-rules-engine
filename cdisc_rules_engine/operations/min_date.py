import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface


class MinDate(OperationInterface):
    def execute(self) -> pd.DataFrame:
        if not self.params.grouping:
            data = pd.to_datetime(self.params.dataframe[self.params.target])
            min_date = data.min()
            if isinstance(min_date, pd._libs.tslibs.nattype.NaTType):
                result = ""
            else:
                result = min_date.isoformat()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).min()
        return self._handle_operation_result(result)
