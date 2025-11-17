from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDomainIsCustomOperation(SqlBaseOperation):
    def _execute_operation(self):
        """
        Return a bool indicating whether the domain is custom
        """
        standard_metadata = self.params.standards_context.get_standard_metadata()
        domain_is_custom = self.params.domain not in standard_metadata.get("domains", {})
        return SqlOperationResult(query=f"SELECT {domain_is_custom} AS value", type="constant", subtype="Bool")
