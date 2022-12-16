import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class Distinct(BaseOperation):
    def _execute_operation(self):
        if not self.params.grouping:
            data = self.params.dataframe[self.params.target].unique()
            if isinstance(data[0], bytes):
                data = data.astype(str)
            result = set(data)
        else:
            grouped = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            )
            result = grouped[self.params.target].agg(
                lambda x: pd.Series([set(x.unique())])
            )
        return result
