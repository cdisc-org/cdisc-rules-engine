from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlStandardDomainsOperation(SqlBaseOperation):
    def _execute_operation(self):
        """
        Return a list of the standard domains
        """
        standard_domains = list(self.params.standards_context.get_standard_metadata().get("domains", {}))

        query = self._format_variable_list_to_query(vars=standard_domains, unique=True, ordered=True)

        return SqlOperationResult(query=query, type="collection", subtype="Char")
