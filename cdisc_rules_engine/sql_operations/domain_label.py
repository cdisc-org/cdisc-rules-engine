from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDomainLabelOperation(SqlBaseOperation):
    def _execute_operation(self):
        """
        Return a SQL query that produces the domain label for the currently executing domain within the standards
        """
        label = self.params.standards_context.get_domain_label(self.params.domain)
        return SqlOperationResult(
            query=f"SELECT '{label.replace('\'', '\'\'')}' AS value", type="constant", subtype="Char"
        )
