from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDatasetNamesOperation(SqlBaseOperation):
    def _execute_operation(self):
        all_tables = self.data_service.pgi.schema.get_tables()
        source_tables = [name for name, schema in all_tables if schema.source == "data"]
        query = self._format_variable_list_to_query(vars=source_tables)

        return SqlOperationResult(query=query, type="collection", subtype="Char")
