from cdisc_rules_engine.constants.metadata_columns import METADATA_COLUMNS
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

import logging

logger = logging.getLogger(__name__)


class SqlDatasetColumnOrderOperation(SqlBaseOperation):

    def _execute_operation(self):
        dataset_name = self.params.domain.lower()
        all_tables = self.data_service.pgi.schema.get_tables()

        dataset_schema = next((schema for name, schema in all_tables if name == dataset_name), None)
        if dataset_schema is None:
            raise ValueError(f"Dataset '{dataset_name}' not found in schema.")

        columns = dataset_schema.get_columns()

        # Filter out metadata columns and 'id' column, and convert to uppercase
        column_names = [col[0].upper() for col in columns if col[0].upper() not in METADATA_COLUMNS and col[0] != "id"]

        if column_names:
            # Format column names for SQL ARRAY
            formatted_cols = [f"'{name}'" for name in column_names]
            array_str = f"ARRAY[{', '.join(formatted_cols)}]"
            query = f"SELECT {array_str} AS value"
        else:
            # Return empty array
            query = "SELECT ARRAY[]::text[] AS value"

        return SqlOperationResult(query, type="collection", subtype="Char")
