from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDistinctOperation(SqlBaseOperation):
    def _execute_operation(self):
        dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
        column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, self.params.target)
        column_type = self.data_service.pgi.schema.get_column(self.params.domain, self.params.target).type
        if not self.params.grouping:

            query = f"SELECT DISTINCT {column_id} AS value FROM {dataset_id}"
            return SqlOperationResult(query=query, type="collection", subtype=column_type)
        else:
            grouping_columns = [
                self.data_service.pgi.schema.get_column(self.params.domain, group) for group in self.params.grouping
            ]

            groups_select = ", ".join([f"{col.hash} AS {col.name}" for col in grouping_columns])
            groups_group_by = ", ".join([col.hash for col in grouping_columns])

            query = f"""SELECT
                            {groups_select}, {column_id} AS value
                        FROM {dataset_id}
                        GROUP BY {groups_group_by}, {column_id}
                        ORDER BY {groups_group_by}, {column_id}"""
            return SqlOperationResult(query=query, type="table", subtype=column_type)
