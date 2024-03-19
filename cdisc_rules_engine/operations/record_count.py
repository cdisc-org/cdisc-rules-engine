from .base_operation import BaseOperation


class RecordCount(BaseOperation):
    def _execute_operation(self, filter: dict = None) -> int:
        """
        Returns number of records in the dataset as datatype: int64
        """
        dataframe = self.params.dataframe
        filter_exp = ""
        if filter:
            for variable, value in filter.items():
                if filter_exp:
                    filter_exp += " & "
                filter_exp += f"{variable} == '{value}'"
        if filter_exp:
            dataframe = dataframe.query(filter_exp)
        record_count: int = len(dataframe)
        breakpoint()
        return record_count
