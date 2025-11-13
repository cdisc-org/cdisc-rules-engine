from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlVariableCountOperation(SqlBaseOperation):
    def _execute_operation(self):
        all_dataset_metadata = [
            self.data_service.get_dataset_metadata(ds_id) for ds_id in self.data_service.get_uploaded_dataset_ids()
        ]

        target = self.params.target
        domain = self.params.domain.upper()

        if target.upper().startswith(domain):
            wildcard_target = "--" + target[len(domain) :]
        else:
            wildcard_target = target

        count = 0
        for dataset_metadata in all_dataset_metadata:
            if wildcard_target.startswith("--"):
                target_variable = dataset_metadata.name.upper() + wildcard_target[2:]
            else:
                target_variable = wildcard_target

            if any([target_variable == var.name for var in dataset_metadata.variables]):
                count += 1

        query = f"SELECT {count} AS value"
        return SqlOperationResult(query=query, type="constant", subtype="Num")
