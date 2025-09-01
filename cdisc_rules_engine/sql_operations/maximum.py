from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlMaximum(SqlBaseOperation):
    def _execute_operation(self):
        if not self.params.grouping:
            dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
            column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, self.params.target)
            return f"(SELECT MAX({column_id}) FROM {dataset_id})"
        else:
            """result = self.params.dataframe.groupby(
                self.params.grouping, as_index=False
            ).data.max()"""
            raise NotImplementedError("Grouping functionality is not implemented yet.")
