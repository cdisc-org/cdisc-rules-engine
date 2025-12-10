import pandas as pd
from cdisc_rules_engine.operations.base_operation import BaseOperation


class MaxDate(BaseOperation):
    def _execute_operation(self):
        if not self.params.grouping:
            data = pd.to_datetime(self.params.dataframe[self.params.target])
            max_date = data.max()
            if isinstance(max_date, pd._libs.tslibs.nattype.NaTType):
                result = ""
            else:
                result = max_date.isoformat()
        else:
            grouped = self.params.dataframe.groupby(
                self.params.grouping, as_index=False, group_keys=False
            )
            result = grouped.data[self.params.target].agg(self._max_date_for_column)
        if isinstance(result, pd.Series):
            result = result.apply(lambda x: x.isoformat() if pd.notna(x) else "")
        elif isinstance(result, pd.DataFrame):
            for col in result.columns:
                if pd.api.types.is_datetime64_any_dtype(result[col]):
                    result[col] = result[col].apply(
                        lambda x: x.isoformat() if pd.notna(x) else ""
                    )
        return result

    def _max_date_for_column(self, column) -> pd.Series:
        return pd.Series(
            {self.params.operation_id: pd.to_datetime(column).dt.date.max().isoformat()}
        )
