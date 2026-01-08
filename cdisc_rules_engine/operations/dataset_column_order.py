from cdisc_rules_engine.constants.metadata_columns import METADATA_COLUMNS
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
        return [
            column
            for column in self.params.dataframe.columns.to_list()
            if column not in METADATA_COLUMNS
        ]
