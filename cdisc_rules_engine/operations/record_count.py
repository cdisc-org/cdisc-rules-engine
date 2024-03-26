# import pandas as pd
from itertools import chain

from cdisc_rules_engine.operations.base_operation import BaseOperation


class RecordCount(BaseOperation):
    def _execute_operation(self) -> int:
        """
        Returns number of records in the dataset as datatype: int64
        """
        dataframe = self.params.dataframe
        filter_exp = ""
        if self.params.dataframe.groupby:
            grouped = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            )
        if self.params.filter:
            for variable, value in self.params.filter.items():
                if filter_exp:
                    filter_exp += " & "
                filter_exp += f"{variable} == '{value}'"
            if filter_exp:
                filtered = dataframe.query(filter_exp)
        matching_keys = [
            key
            for key, values in grouped.groups.items()
            if any(value in filtered.index for value in values)
        ]
        matching_values = [grouped.groups[key] for key in matching_keys]
        flattened_indexes = list(chain.from_iterable(matching_values))
        print(flattened_indexes)
        breakpoint()

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

        #     grouped_DF = self.params.dataframe.groupby(
        #         self.params.grouping, as_index=False)
        #     if self.params.filter:
        #         for variable, value in self.params.filter.items():
        #             if filter_exp:
        #                 filter_exp += " & "
        #             filter_exp += f"{variable} == '{value}'"
        #         if filter_exp:
        #             dataframe = dataframe.query(filter_exp)
        #     dataframe = pd.merge(dataframe, grouped_DF[self.params.grouping],
        # on=self.params.grouping, how='inner')
        # record_count = len(dataframe)
        # return record_count
