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
        table = self.params.table if self.params.use_rule_type_table else self.params.domain
        dataset_id = self.data_service.pgi.schema.get_table_hash(table)

        # Special case for counting size of whole dataset
        if self.params.target is None:
            column_id = "*"
        else:
            column_id = self.data_service.pgi.schema.get_column_hash(table, self.params.target)

        where_clause = self.construct_where_clause()

        if not self.params.grouping:
            query = f"SELECT {self.function}({column_id}) AS value FROM {dataset_id} {where_clause}"
            return SqlOperationResult(query=query, type="constant", subtype="Num")
        else:
            grouping_columns = [self.data_service.pgi.schema.get_column(table, group) for group in self.params.grouping]

            where_conditions = []
            params = {}
            for i, col in enumerate(grouping_columns):
                param_name = f"${i + 1}"
                where_conditions.append(f"({col.hash} = {param_name} OR ({col.hash} IS NULL AND {param_name} IS NULL))")
                params[param_name] = col.name

            where_clause_parts = []
            if where_clause.strip():
                where_clause_parts.append(where_clause.replace("WHERE", "").strip())
            where_clause_parts.extend(where_conditions)

            combined_where = "WHERE " + " AND ".join(where_clause_parts)

            query = f"""SELECT {self.function}({column_id}) AS value
                        FROM {dataset_id}
                        {combined_where}"""
            return SqlOperationResult(query=query, type="constant", subtype="Num", params=params)
