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
        copy_dataframe = self.params.dataframe.copy()
        filter_exp = ""
        self.params.target = "size"
        if not self.params.grouping and not self.params.filter:
            return len(copy_dataframe)
        if self.params.filter:
            for variable, value in self.params.filter.items():
                if filter_exp:
                    filter_exp += " & "
                filter_exp += f"{variable} == '{value}'"
                if filter_exp:
                    filtered = copy_dataframe.query(filter_exp)
            if self.params.grouping:
                result = copy_dataframe.groupby(
                    self.params.grouping, as_index=False
                ).size()
                filtered = filtered[self.params.grouping]
                filtered_group = result[
                    result[self.params.grouping[0]].isin(
                        filtered[self.params.grouping[0]]
                    )
                ]
                return filtered_group
        if self.params.grouping:
            return copy_dataframe.groupby(self.params.grouping, as_index=False).size()
        return len(filtered)
