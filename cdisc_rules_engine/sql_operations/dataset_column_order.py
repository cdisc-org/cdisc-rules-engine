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

        if column_names:
            # Format column names for SQL VALUES clause to return individual rows
            formatted_cols = [f"('{name}')" for name in column_names]
            values_clause = ", ".join(formatted_cols)
            query = f"SELECT column1 AS value FROM (VALUES {values_clause}) AS t(column1)"
        else:
            # Return empty result set using VALUES with no rows
            query = "SELECT column1 AS value FROM (VALUES (NULL)) AS t(column1) WHERE FALSE"

        return SqlOperationResult(query, type="collection", subtype="Char")
