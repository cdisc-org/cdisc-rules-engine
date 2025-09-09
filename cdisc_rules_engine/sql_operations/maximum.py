from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlMaximum(SqlBaseOperation):
    def _execute_operation(self):
        dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
        column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, self.params.target)
        if not self.params.grouping:
            query = f"SELECT MAX({column_id}) AS value FROM {dataset_id}"
            return SqlOperationResult(query=query, type="constant")
        else:
            grouping_columns = [
                self.data_service.pgi.schema.get_column(self.params.domain, group) for group in self.params.grouping
            ]

            groups_select = ", ".join([f"{col.hash} AS {col.name}" for col in grouping_columns])
            groups_group_by = ", ".join([col.hash for col in grouping_columns])

            query = f"""SELECT
                            {groups_select}, MAX({column_id}) AS value
                        FROM {dataset_id}
                        GROUP BY {groups_group_by}
                        ORDER BY {groups_group_by}"""
            return SqlOperationResult(query=query, type="table")
