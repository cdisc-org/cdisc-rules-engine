import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class Distinct(BaseOperation):
    def _execute_operation(self):
        result = self.params.dataframe
        if self.params.filter:
            result = self._filter_data(result)
        if not self.params.grouping:
            data = result[self.params.target].unique()
            if isinstance(data[0], bytes):
                data = data.astype(str)
            result = set(data)
        else:
            grouped = result.groupby(
                self.params.grouping, as_index=False, group_keys=False
            ).data
            if isinstance(result.data, pd.DataFrame):
                result = grouped[self.params.target].agg(self._unique_values_for_column)
            else:
                result = (
                    grouped[self.params.target]
                    .unique()
                    .rename({self.params.target: self.params.operation_id})
                )
                result = result.apply(set).to_frame().reset_index()
        return result

    def _unique_values_for_column(self, column):
        return pd.Series({self.params.operation_id: set(column.unique())})
