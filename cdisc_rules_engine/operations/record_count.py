import pandas as pd
import re
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
            grouping_for_operations = (
                self._get_grouping_for_operations()
                if self.params.regex
                else effective_grouping
            )
            if self.params.regex and filtered is None:
                group_df = self._get_regex_grouped_counts(
                    self.params.dataframe, grouping_for_operations
                )
            else:
                group_df = self.params.dataframe.get_grouped_size(
                    effective_grouping, as_index=False, dropna=False
                )
            if filtered is not None:
                if self.params.regex:
                    filtered_grouped = self._get_regex_grouped_counts(
                        filtered, grouping_for_operations
                    )
                else:
                    filtered_grouped = filtered.get_grouped_size(
                        effective_grouping, as_index=False
                    )
                group_df = (
                    group_df[
                        (
                            grouping_for_operations
                            if self.params.regex
                            else effective_grouping
                        )
                    ]
                    .merge(
                        filtered_grouped,
                        on=(
                            grouping_for_operations
                            if self.params.regex
                            else effective_grouping
                        ),
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

    def _get_grouping_for_operations(self) -> list:
        grouping_cols = (
            self.params.grouping
            if isinstance(self.params.grouping, list)
            else [self.params.grouping]
        )
        effective_grouping = []
        for col in grouping_cols:
            col = self._resolve_variable_name(col, self.params.domain)
            if col in self.evaluation_dataset.data.columns:
                sample_val = self.evaluation_dataset[col].iloc[0]
                if isinstance(sample_val, (list, tuple)):
                    effective_grouping.extend(sample_val)
                else:
                    effective_grouping.append(col)
            else:
                effective_grouping.append(col)
        return list(dict.fromkeys(effective_grouping))

    def _get_regex_grouped_counts(self, dataframe, grouping_columns):
        df_for_grouping = self._apply_regex_to_grouping_columns(
            dataframe, grouping_columns
        )
        grouped_counts = df_for_grouping.groupby(
            grouping_columns, as_index=False, dropna=False
        ).size()
        for col in grouping_columns:
            if col in grouped_counts.columns and col in df_for_grouping.columns:
                grouped_counts[col] = grouped_counts[col].astype(
                    df_for_grouping[col].dtype
                )
        transformed_with_counts = df_for_grouping.merge(
            grouped_counts, on=grouping_columns, how="left"
        )
        result = dataframe[grouping_columns].copy()
        result["size"] = transformed_with_counts["size"].values
        result = result.groupby(grouping_columns, as_index=False, dropna=False).first()
        return result

    def _apply_regex_to_grouping_columns(
        self, dataframe: pd.DataFrame, grouping_columns: list
    ) -> pd.DataFrame:
        df_subset = dataframe[grouping_columns].copy()
        for col in grouping_columns:
            if col in df_subset.columns:
                sample_values = df_subset[col].dropna()
                if not sample_values.empty:
                    sample_value = sample_values.iloc[0]
                    if isinstance(sample_value, str):
                        try:
                            if re.match(self.params.regex, sample_value):
                                df_subset[col] = df_subset[col].apply(
                                    lambda x: (
                                        self._apply_regex_pattern(x)
                                        if isinstance(x, str) and x
                                        else x
                                    )
                                )
                        except re.error:
                            pass
        return df_subset

    def _apply_regex_pattern(self, value: str) -> str:
        match = re.match(self.params.regex, value)
        return match.group(0) if match else value

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
            col = self._resolve_variable_name(col, self.params.domain)
            if col in self.evaluation_dataset.data.columns:
                sample_val = self.evaluation_dataset[col].iloc[0]
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
