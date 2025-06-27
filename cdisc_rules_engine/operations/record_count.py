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
            effective_grouping, all_na_cols = self._build_effective_grouping()
            group_df = self.params.dataframe.get_grouped_size(
                effective_grouping, as_index=False, dropna=False
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
            for col, original_value in all_na_cols.items():
                group_df[col] = original_value
                group_df[col] = group_df[col].astype(self.params.dataframe[col].dtype)
            return group_df
        return result

    def _build_effective_grouping(self) -> tuple[list, dict]:
        """
        Build effective grouping by expanding operation results used to group and track all-NA columns to revert them.
        NA columns are completely empty and pandas converts them to NaN in the grouping.
        Returns: (effective_grouping, all_na_cols)
        """
        grouping_cols = (
            self.params.grouping
            if isinstance(self.params.grouping, list)
            else [self.params.grouping]
        )
        effective_grouping = []
        for col in grouping_cols:
            if col in self.params.dataframe.columns:
                sample_val = self.params.dataframe[col].iloc[0]
                if isinstance(sample_val, (list, tuple)):
                    # This is an operation result - expand the list
                    effective_grouping.extend(sample_val)
                else:
                    effective_grouping.append(col)
            else:
                effective_grouping.append(col)
        effective_grouping = list(dict.fromkeys(effective_grouping))
        all_na_cols = {}
        for col in effective_grouping:
            if col in self.params.dataframe.columns:
                if self.params.dataframe[col].isna().all():
                    all_na_cols[col] = None
                elif (
                    self.params.dataframe[col].dtype == "object"
                    and self.params.dataframe[col].fillna("").str.strip().eq("").all()
                ):
                    all_na_cols[col] = ""
        return effective_grouping, all_na_cols
