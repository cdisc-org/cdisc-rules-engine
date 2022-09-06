import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class MinDate(BaseOperation):
    def _execute_operation(self):
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
        return result
