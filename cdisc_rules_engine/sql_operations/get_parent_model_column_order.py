from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from typing import List


class SqlGetParentModelColumnOrderOperation(SqlBaseOperation):
    def _execute_operation(self):

        parent_col_list = self._get_model_variables(self._get_rdomain())

        # Convert the list to individual rows in SQL
        if parent_col_list and isinstance(parent_col_list, list):
            table_values_clause = ", ".join([f"('{col}')" for col in parent_col_list])
            query = f"SELECT column1 AS value FROM (VALUES {table_values_clause}) AS t(column1)"
        else:
            # Return empty result set using VALUES with no rows - this is a valid empty table
            query = "SELECT column1 AS value FROM (VALUES (NULL)) AS t(column1) WHERE FALSE"

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_model_variables(self, domain):
        try:
            model_variables: List[dict] = self._get_variables_metadata_from_standard_model(domain)

            # Replace wildcards and extract variable names
            variable_names_list = self._replace_variable_wildcards(model_variables, domain)

            return variable_names_list

        except Exception as e:
            # If the metadata retrieval fails, the rule can't run, so throwing error
            raise Exception(f"Metadata retrieval failed due to error: {str(e)}")

    def _get_rdomain(self):
        rdomain = self.data_service.get_dataset_metadata(self.params.domain).rdomain
        return rdomain
