from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDistinctOperation(SqlBaseOperation):
    def _execute_operation(self):
        dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)
        column_id = self.data_service.pgi.schema.get_column_hash(self.params.domain, self.params.target)
        column_type = self.data_service.pgi.schema.get_column(self.params.domain, self.params.target).type

        where_conditions = []
        params = {}

        if self.params.grouping:
            grouping_columns = [
                self.data_service.pgi.schema.get_column(self.params.domain, group) for group in self.params.grouping
            ]
            for i, col in enumerate(grouping_columns):
                param_name = f"${i + 1}"
                where_conditions.append(f"{col.hash} = {param_name}")
                params[param_name] = col.name

        if self.params.filter:
            for k, v in self.params.filter.items():
                filter_col = self.data_service.pgi.schema.get_column_hash(self.params.domain, k)
                if not filter_col:
                    raise ValueError(f"Filter column '{k}' not found in domain '{self.params.domain}'")
                where_conditions.append(f"{filter_col} = '{v}'")

        query = f"SELECT DISTINCT {column_id} AS value FROM {dataset_id}"

        if where_conditions:
            query += f" WHERE {' AND '.join(where_conditions)}"

        return SqlOperationResult(
            query=query, type="collection", subtype=column_type, params=params if params else None
        )
