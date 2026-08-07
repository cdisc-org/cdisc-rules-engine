import pandas as pd
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.operations.base_operation import BaseOperation


class ValueEquals(BaseOperation):
    def _execute_operation(self):
        dataframe = self.params.dataframe
        target = self.params.target
        expected = self.params.value

        if target not in dataframe.columns:
            raise ValueError(f"Target column '{target}' not found in dataset")

        target_series = dataframe[target]

        if self._is_variable_list_column(target_series):
            # Avoid dask row-wise apply tokenization issues by using pandas for this path.
            if isinstance(dataframe, DaskDataset):
                pandas_df = dataframe.data.compute()
                return pandas_df.apply(
                    self._get_matching_variable_names_from_row,
                    axis=1,
                    args=(target, expected),
                )
            return dataframe.apply(
                self._get_matching_variable_names_from_row,
                axis=1,
                args=(target, expected),
            )

        return target_series.apply(
            lambda value: [target] if self._values_equal(value, expected) else []
        )

    def _is_variable_list_column(self, series) -> bool:
        non_null_values = series[series.notna()]
        return len(non_null_values) > 0 and all(
            isinstance(value, (list, tuple, set)) for value in non_null_values
        )

    def _get_matching_variable_names_from_row(
        self, row, list_column_name: str, expected
    ):
        variable_names = row[list_column_name]
        if not isinstance(variable_names, (list, tuple, set)):
            return []

        matches = []
        for variable_name in variable_names:
            if variable_name in row.index and self._values_equal(
                row[variable_name], expected
            ):
                matches.append(variable_name)
        return matches

    def _values_equal(self, actual, expected) -> bool:
        if expected is None:
            return pd.isna(actual)
        return actual == expected