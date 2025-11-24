from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from typing import List


class SqlGetModelFilteredVariables(SqlBaseOperation):

    def _execute_operation(self):
        """
        Fetches variables from the CDISC library model that match the specified filter criteria.
        Similar to LibraryModelVariablesFilter but for SQL operations.

        Filters variables based on key_name and key_value parameters.
        For example: key_name="role", key_value="Timing" would return timing variables.

        Returns a SQL query that produces the filtered variable names as individual rows.
        """
        # Get model variables and filter them (even if key/val are empty, this will return empty list)
        model_variables = self._get_model_filtered_variables()

        query = self._format_variable_list_to_query(vars=model_variables)

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_model_filtered_variables(self):
        """
        Get variables metadata from standard model and filter by key_name/key_value.

        This is the SQL equivalent of the original operation's _get_model_filtered_variables method.
        """
        key = self.params.key_name
        val = self.params.key_value

        # Return empty list if no filter criteria provided
        if not key or not val:
            return []

        try:
            model_variables: List[dict] = self._get_variables_metadata_from_standard_model(self.params.domain)

            # Filter variables by the specified key/value criteria
            filtered_model = [var for var in model_variables if var.get(key) == val]

            # Replace wildcards and extract variable names
            variable_names_list = self._replace_variable_wildcards(filtered_model, self.params.domain)

            return variable_names_list

        except Exception:
            # Return empty list on error (SQL operations should not return error strings)
            return []
