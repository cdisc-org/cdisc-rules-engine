import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class MaxDate(BaseOperation):
    def _execute_operation(self):
        if not self.params.grouping:
            data = pd.to_datetime(self.params.dataframe[self.params.target])
            max_date = data.max()
            if isinstance(max_date, pd._libs.tslibs.nattype.NaTType):
                result = ""
            else:
                result = max_date.isoformat()
        else:
            result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).max()
        return result
