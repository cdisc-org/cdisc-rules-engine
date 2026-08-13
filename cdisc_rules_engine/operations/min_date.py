import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import format_date


class MinDate(BaseOperation):
    def _execute_operation(self):
        data = pd.to_datetime(
            self.params.dataframe[self.params.target], format="ISO8601"
        )
        if not self.params.grouping:
            min_date = data.min()
            if isinstance(min_date, pd._libs.tslibs.nattype.NaTType):
                result = ""
            else:
                result = format_date(min_date)
        else:
            grouping_cols = self.params.grouping
            if isinstance(grouping_cols, str):
                grouping_cols = [grouping_cols]
            group_keys = [self.params.dataframe[col] for col in grouping_cols]
            min_dates = data.groupby(group_keys).transform("min")
            result = min_dates.apply(format_date)
        return result
