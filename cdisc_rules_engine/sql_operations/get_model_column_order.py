from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from typing import List


class SqlGetModelColumnOrder(SqlBaseOperation):

    def _execute_operation(self):

        model_variables = self._get_model_variables()

        # Convert the list to individual rows in SQL
        if model_variables and isinstance(model_variables, list):
            # Format variable names for SQL VALUES clause, escaping single quotes
            formatted_vars = [f"('{var.replace(chr(39), chr(39) + chr(39))}')" for var in model_variables]
            values_clause = ", ".join(formatted_vars)
            query = f"SELECT column1 AS value FROM (VALUES {values_clause}) AS t(column1)"
        else:
            # Return empty result set using VALUES with no rows - this is a valid empty table
            query = "SELECT column1 AS value FROM (VALUES (NULL)) AS t(column1) WHERE FALSE"

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_model_variables(self):
        try:
            model_variables: List[dict] = self._get_variables_metadata_from_standard_model(self.params.domain)

            # Replace wildcards and extract variable names
            variable_names_list = self._replace_variable_wildcards(model_variables, self.params.domain)

            return variable_names_list

        except Exception as e:
            # If the metadata retrieval fails, the rule can't run, so throwing error
            raise Exception(f"Metadata retrieval failed due to error: {str(e)}")
