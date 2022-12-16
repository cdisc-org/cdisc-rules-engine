from cdisc_rules_engine.operations.base_operation import BaseOperation


class DatasetColumnOrder(BaseOperation):
    def _execute_operation(self):
        """
        Returns dataset columns as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        """
        return self.params.dataframe.columns.to_list()
