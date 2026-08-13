import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import format_date


class MaxDate(BaseOperation):
    def _execute_operation(self):
        data = pd.to_datetime(
            self.params.dataframe[self.params.target], format="ISO8601"
        )
        if not self.params.grouping:
            max_date = data.max()
            if isinstance(max_date, pd._libs.tslibs.nattype.NaTType):
                result = ""
            else:
                result = format_date(max_date)
            return pd.Series(result, index=self.evaluation_dataset.index)
        grouping_cols = self.params.grouping
        if isinstance(grouping_cols, str):
            grouping_cols = [grouping_cols]

        group_keys = [self.params.dataframe[col] for col in grouping_cols]
        max_dates = data.groupby(group_keys).max().apply(format_date)

        # Build lookup: single key -> scalar dict, multi-key -> tuple-keyed dict
        if len(grouping_cols) == 1:
            lookup_keys = self.evaluation_dataset[grouping_cols[0]]
        else:
            lookup_keys = pd.Series(
                list(zip(*[self.evaluation_dataset[c] for c in grouping_cols])),
                index=self.evaluation_dataset.index,
            )

        result = lookup_keys.map(max_dates).fillna("")
        result.index = self.evaluation_dataset.index
        return result
