from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDatasetNames(SqlBaseOperation):
    def _execute_operation(self):
        all_tables = self.data_service.pgi.schema.get_tables()
        source_tables = [name for name, schema in all_tables if schema.source == "data"]
        table_values_clause = ", ".join([f"('{name}')" for name in source_tables])
        return SqlOperationResult(
            f"SELECT column1 AS value FROM (VALUES {table_values_clause})", type="collection", subtype="Char"
        )
