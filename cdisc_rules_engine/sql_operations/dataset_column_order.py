from cdisc_rules_engine.constants.metadata_columns import METADATA_COLUMNS
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDatasetColumnOrderOperation(SqlBaseOperation):

    def _execute_operation(self):
        dataset_name = self.params.domain.lower()
        all_tables = self.data_service.pgi.schema.get_tables()

        dataset_schema = next((schema for name, schema in all_tables if name == dataset_name), None)
        if dataset_schema is None:
            raise ValueError(f"Dataset '{dataset_name}' not found in schema.")

        columns = dataset_schema.get_columns()

        # Filter out metadata columns and 'id' column
        column_names = [col[0].upper() for col in columns if col[0] not in METADATA_COLUMNS and col[0] != "id"]

        query = self._format_variable_list_to_query(vars=column_names)

        return SqlOperationResult(query, type="collection", subtype="Char")
