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
        # if not self.params.grouping:
        #     result = len(self.params.dataframe)
        # else:
        #     result = self.params.dataframe.groupby(
        #         self.params.grouping, as_index=False
        #     ).count()
        # return result

        copy_dataframe = self.params.dataframe.copy()
        filter_exp = ""
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
            copy_dataframe = copy_dataframe.groupby(
                self.params.grouping, as_index=False
            )
            # breakpoint()
            # return copy_dataframe
            matching_keys = [
                key
                for key, values in copy_dataframe.groups.items()
                if any(value in filtered.index for value in values)
            ]
            result = pd.DataFrame()
            for group in matching_keys:
                result = result.append(copy_dataframe.get_group(group))
            # filtered_df = {key: group for key, group in
            #  dataframe if key in matching_keys}
            breakpoint()
            return result
        return len(filtered)

        # merged_df = dataframe.merge(filtered, grouped[self.params.grouping],
        # on=self.params.grouping, how='inner')
        # record_count = len(dataframe)
        # return record_count
        # print(flattened_indexes)

        #  original filter fx
        # if self.params.filter:
        #     for variable, value in self.params.filter.items():
        #         if filter_exp:
        #             filter_exp += " & "
        #         filter_exp += f"{variable} == '{value}'"
        #     if filter_exp:
        #         filtered = dataframe.query(filter_exp)
        # else:
        #     filtered = dataframe
        # if not self.params.grouping:
        #     return len(filtered)
