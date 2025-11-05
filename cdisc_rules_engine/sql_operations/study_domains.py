from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlStudyDomainsOperation(SqlBaseOperation):
    def _execute_operation(self):

        dataset_metadata = self._get_full_dataset_metadata()
        domains = [(dm.domain or "") for dm in dataset_metadata]

        # Convert the list to individual rows in SQL
        if domains and isinstance(domains, list):
            table_values_clause = ", ".join([f"('{domain}')" for domain in set(domains)])
            query = f"SELECT column1 AS value FROM (VALUES {table_values_clause}) AS t(column1)"
        else:
            # Return empty result set using VALUES with no rows - this is a valid empty table
            query = "SELECT column1 AS value FROM (VALUES (NULL)) AS t(column1) WHERE FALSE"

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_full_dataset_metadata(self):
        dataset_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]
        return dataset_metadata
