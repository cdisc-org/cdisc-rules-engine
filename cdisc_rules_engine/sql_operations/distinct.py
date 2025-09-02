import pandas as pd

from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDistinct(SqlBaseOperation):
    def _execute_operation(self):
        if not self.params.grouping:
            dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
            column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, self.params.target)

            query = f"SELECT DISTINCT {column_id} FROM {dataset_id}"
            return SqlOperationResult(query=query, type="collection")
        else:
            """grouped = self.params.dataframe.groupby(self.params.grouping, as_index=False, group_keys=False).data
            if isinstance(self.params.dataframe.data, pd.DataFrame):
                result = grouped[self.params.target].agg(self._unique_values_for_column)
            else:
                result = grouped[self.params.target].unique().rename({self.params.target: self.params.operation_id})
                result = result.apply(set).to_frame().reset_index()"""
            raise NotImplementedError("Grouping functionality is not implemented yet.")

    def _unique_values_for_column(self, column):
        return pd.Series({self.params.operation_id: set(column.unique())})
