from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDomainLabelOperation(SqlBaseOperation):
    def _execute_operation(self):
        """
        Return a SQL query that produces the domain label for the currently executing domain
        """
        try:
            metadata = self.data_service.get_dataset_metadata(self.params.domain.lower())
            domain_label = metadata.dataset_label
            if domain_label:
                return SqlOperationResult(
                    query=f"SELECT '{domain_label.replace('\'', '\'\'')}' AS value", type="constant", subtype="Char"
                )
        except Exception:
            # If metadata retrieval fails, fall back to empty label
            pass

        # Return empty label if no valid label found
        return SqlOperationResult(query="SELECT '' AS value", type="constant", subtype="Char")
