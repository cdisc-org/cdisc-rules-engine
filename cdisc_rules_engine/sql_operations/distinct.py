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

            where_conditions = []
            params = {}
            for i, col in enumerate(grouping_columns):
                param_name = f"${i + 1}"
                where_conditions.append(f"{col.hash} = {param_name}")
                params[param_name] = col.name

            where_clause = " AND ".join(where_conditions)

            query = f"""SELECT DISTINCT {column_id} AS value
                        FROM {dataset_id}
                        WHERE {where_clause}"""
            return SqlOperationResult(query=query, type="collection", subtype=column_type, params=params)
