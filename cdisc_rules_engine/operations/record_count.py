import pandas as pd

from cdisc_rules_engine.operations.base_operation import BaseOperation


class RecordCount(BaseOperation):
    def _execute_operation(self) -> pd.Series:
        """
        Returns number of records in the dataset as pd.Series like:
        0    5
        1    5
        2    5
        3    5
        4    5
        dtype: int64
        """
        filtered = None
        result = len(self.params.dataframe)
        if self.params.filter:
            filtered = self._filter_data(self.params.dataframe)
            result = len(filtered)
        if self.params.grouping:
            self.params.target = "size"
            group_df = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).data.size()
            if filtered is not None:
                group_df = (
                    group_df[self.params.grouping]
                    .merge(
                        filtered.groupby(
                            self.params.grouping, as_index=False
                        ).data.size(),
                        on=self.params.grouping,
                        how="left",
                    )
                    .fillna(0)
                )
            return group_df
        return result
