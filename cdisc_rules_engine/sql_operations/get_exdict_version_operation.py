from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlGetExternalDictionaryVersionOperation(SqlBaseOperation):

    def _execute_operation(self):
        external_dictionary_type = self.params.external_dictionary_type

        metadata = (self.data_service.dictionary_metadata or {}).get(external_dictionary_type)
        version = getattr(metadata, "version", None)

        if not version:
            raise Exception(
                f"Version for external dictionary type {external_dictionary_type} "
                f"is not found in the provided external dictionaries."
            )

        return SqlOperationResult(query=f"SELECT '{version}' AS value", type="constant", subtype="Char")
