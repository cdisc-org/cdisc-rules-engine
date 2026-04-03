from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlMinusOperation(SqlBaseOperation):
    def _execute_operation(self):
        """
        Executes a set difference (MINUS/EXCEPT) operation.
        Returns the values present in 'name' that are NOT present in 'subtract'.
        """
        domain = self.params.domain
        dataset_id = self.data_service.pgi.schema.get_table_hash(domain)

        name_param = self.params.name
        subtract_param = self.params.subtract

        name_query = self._resolve_param(name_param, domain, dataset_id)
        subtract_query = self._resolve_param(subtract_param, domain, dataset_id)

        query = f"""
            SELECT value FROM ({name_query}) AS name_q
            EXCEPT
            SELECT value FROM ({subtract_query}) AS subtract_q
        """

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _resolve_param(self, param_val: str, domain: str, dataset_id: str) -> str:
        if self._column_exists_in_domain(domain, param_val):
            col_hash = self.data_service.pgi.schema.get_column_hash(domain, param_val)
            return f"SELECT {col_hash} AS value FROM {dataset_id} WHERE {col_hash} IS NOT NULL"
        elif self._get_previous_operation(param_val):
            return self._get_previous_operation(param_val).query
