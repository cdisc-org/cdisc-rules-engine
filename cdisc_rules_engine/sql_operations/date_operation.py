from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDateOperation(SqlBaseOperation):
    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService, function: str):
        super().__init__(params, data_service)
        self.function = function

    def _execute_operation(self):
        dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
        column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, self.params.target)
        column_type = self.data_service.pgi.schema.get_column(self.params.domain, self.params.target).type

        # Construct the base WHERE clause from filters if any
        where_clause = self.construct_where_clause()

        if not self.params.grouping:
            query = f"""SELECT COALESCE(
                            CASE
                                WHEN {self.function}(CASE
                                    WHEN {column_id} IS NULL OR {column_id} = '' THEN NULL
                                    ELSE {column_id}::date
                                END) IS NULL THEN ''
                                ELSE TO_CHAR({self.function}(CASE
                                    WHEN {column_id} IS NULL OR {column_id} = '' THEN NULL
                                    ELSE {column_id}::date
                                END), 'YYYY-MM-DD')
                            END, ''
                        ) AS value
                        FROM {dataset_id}"""

            if where_clause:
                query += f" {where_clause}"

            return SqlOperationResult(query=query, type="constant", subtype=column_type)
        else:
            grouping_columns = [
                self.data_service.pgi.schema.get_column(self.params.domain, group) for group in self.params.grouping
            ]

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

            combined_where = "WHERE " + " AND ".join(where_clause_parts) if where_clause_parts else ""

            query = f"""SELECT COALESCE(
                            CASE
                                WHEN {self.function}(CASE
                                    WHEN {column_id} IS NULL OR {column_id} = '' THEN NULL
                                    ELSE {column_id}::date
                                END) IS NULL THEN ''
                                ELSE TO_CHAR({self.function}(CASE
                                    WHEN {column_id} IS NULL OR {column_id} = '' THEN NULL
                                    ELSE {column_id}::date
                                END), 'YYYY-MM-DD')
                            END, ''
                        ) AS value
                        FROM {dataset_id}
                        {combined_where}"""

            return SqlOperationResult(query=query, type="constant", subtype=column_type, params=params)
