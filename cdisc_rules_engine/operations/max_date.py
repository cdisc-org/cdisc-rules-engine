import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.check_operators.helpers import format_date_preserving_precision


class MaxDate(BaseOperation):
    def _execute_operation(self):
        original = self.params.dataframe[self.params.target]
        data = pd.to_datetime(original, format="ISO8601")

        if not self.params.grouping:
            if data.isna().all():
                result = ""
            else:
                max_idx = data.idxmax()
                result = format_date_preserving_precision(original.loc[max_idx])
            return pd.Series(result, index=self.evaluation_dataset.index)

        grouping_cols = self.params.grouping
        if isinstance(grouping_cols, str):
            grouping_cols = [grouping_cols]

        group_keys = [self.params.dataframe[col] for col in grouping_cols]
        idx_of_max = data.groupby(group_keys).apply(
            lambda s: s.idxmax() if s.notna().any() else pd.NA
        )
        max_dates = idx_of_max.apply(
            lambda idx: (
                ""
                if pd.isna(idx)
                else format_date_preserving_precision(original.loc[idx])
            )
        )
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
