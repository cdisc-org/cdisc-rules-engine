import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


def _get_value_from_reference(row, target_col_name):
    ref_col_name = row[target_col_name]
    if pd.notna(ref_col_name) and ref_col_name in row.index:
        return row[ref_col_name]
    return None


class Distinct(BaseOperation):
    def _execute_operation(self):
        result = self.params.dataframe
        if self.params.filter:
            result = self._filter_data(result)
        value_is_reference = getattr(self.params, "value_is_reference", False)
        if not self.params.grouping:
            if value_is_reference:
                target = self.params.target
                data = result.apply(
                    lambda row: _get_value_from_reference(row, target), axis=1
                )
                data = data.dropna().unique()
            else:
                data = result[self.params.target].unique()
            if len(data) > 0 and isinstance(data[0], bytes):
                data = data.astype(str)
            result = set(data)
        else:
            grouped = result.groupby(
                self.params.grouping, as_index=False, group_keys=False
            )
            if value_is_reference:
                target = self.params.target
                operation_id = self.params.operation_id

                def get_referenced_unique_values(group):
                    values = group.apply(
                        lambda row: _get_value_from_reference(row, target), axis=1
                    )
                    return pd.Series({operation_id: set(values.dropna().unique())})

                result = grouped.apply(get_referenced_unique_values).reset_index()
            elif isinstance(result.data, pd.DataFrame):
                result = grouped.data[self.params.target].agg(
                    self._unique_values_for_column
                )
            else:
                result = (
                    grouped.data[self.params.target]
                    .unique()
                    .rename({self.params.target: self.params.operation_id})
                )
                result = result.apply(set).to_frame().reset_index()
        return result

    def _unique_values_for_column(self, column):
        return pd.Series({self.params.operation_id: set(column.unique())})
