from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableExistsOperation(SqlBaseOperation):
    def _execute_operation(self):
        column_exists = self.data_service.pgi.schema.column_exists(self.params.domain, self.params.target)

        query = f"SELECT {str(column_exists).upper()} AS value"

        return SqlOperationResult(query=query, type="constant", subtype="Bool")
