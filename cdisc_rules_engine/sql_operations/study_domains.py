from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlStudyDomainsOperation(SqlBaseOperation):
    def _execute_operation(self):

        dataset_metadata = self._get_full_dataset_metadata()
        domains = [(dm.domain or "") for dm in dataset_metadata]

        query = self._format_variable_list_to_query(vars=domains, unique=True)

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_full_dataset_metadata(self):
        dataset_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]
        return dataset_metadata
