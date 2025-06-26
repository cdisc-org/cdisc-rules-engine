import pandas as pd
from numpy import int64

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
            if isinstance(self.params.grouping, list):
                effective_grouping = self._build_effective_grouping()
            else:
                effective_grouping = self.params.grouping
            group_df = self.params.dataframe.get_grouped_size(
                effective_grouping, as_index=False
            )
            if filtered is not None:
                group_df = (
                    group_df[effective_grouping]
                    .merge(
                        filtered.get_grouped_size(effective_grouping, as_index=False),
                        on=effective_grouping,
                        how="left",
                    )
                    .fillna(0)
                    .astype({self.params.target: int64})
                )
            if isinstance(self.params.grouping, list):
                missing_cols = [
                    col for col in self.params.grouping if col not in group_df.columns
                ]
                for col in missing_cols:
                    original_col = self.params.dataframe[col]
                    group_df[col] = None
                    group_df[col] = group_df[col].astype(original_col.dtype)
            return group_df
        return result

    def _build_effective_grouping(self) -> list:
        effective_grouping = []
        for col in self.params.grouping:
            if col in self.params.dataframe.columns:
                sample_val = self.params.dataframe[col].iloc[0]
                if isinstance(sample_val, (list, tuple)):
                    effective_grouping.extend(
                        [c for c in sample_val if c in self.params.dataframe.columns]
                    )
                elif not self.params.dataframe[col].isna().all():
                    effective_grouping.append(col)
            else:
                effective_grouping.append(col)
        # remove duplicates while preserving order
        return list(dict.fromkeys(effective_grouping))
