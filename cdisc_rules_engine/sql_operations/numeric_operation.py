from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlNumericOperation(SqlBaseOperation):

    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService, function: str):
        super().__init__(params, data_service)
        self.function = function

    def _execute_operation(self):
        dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)

        # Special case for counting size of whole dataset
        if self.params.target is None:
            column_id = "*"
        else:
            column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, self.params.target)

        where_clause = self.construct_where_clause()

        if not self.params.grouping:
            query = f"SELECT {self.function}({column_id}) AS value FROM {dataset_id} {where_clause}"
            return SqlOperationResult(query=query, type="constant", subtype="Num")
        else:
            grouping_columns = [
                self.data_service.pgi.schema.get_column(self.params.domain, group) for group in self.params.grouping
            ]

            groups_select = ", ".join([f"{col.hash} AS {col.name}" for col in grouping_columns])
            groups_group_by = ", ".join([col.hash for col in grouping_columns])

            query = f"""SELECT
                            {groups_select}, {self.function}({column_id}) AS value
                        FROM {dataset_id}
                        {where_clause}
                        GROUP BY {groups_group_by}
                        ORDER BY {groups_group_by}"""
            return SqlOperationResult(query=query, type="table", subtype="Num")
