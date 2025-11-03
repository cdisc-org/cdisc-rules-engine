from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.constants.permissibility import (
    REQUIRED,
    PERMISSIBLE,
    REQUIRED_MODEL_VARIABLES,
    SEQ_VARIABLE,
    PERMISSIBILITY_KEY,
)
from typing import List


class SqlPermissibilityOperation(SqlBaseOperation):

    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService, permissibility: str):
        super().__init__(params, data_service)
        self.permissibility = permissibility

    def _execute_operation(self):

        perm_variables = self._get_perm_variables()

        # Convert the list to individual rows in SQL
        if perm_variables and isinstance(perm_variables, list):
            # Format variable names for SQL VALUES clause, escaping single quotes
            formatted_vars = [f"('{var.replace(chr(39), chr(39) + chr(39))}')" for var in perm_variables]
            values_clause = ", ".join(formatted_vars)
            query = f"SELECT column1 AS value FROM (VALUES {values_clause}) AS t(column1)"
        else:
            # Return empty result set using VALUES with no rows - this is a valid empty table
            query = "SELECT column1 AS value FROM (VALUES (NULL)) AS t(column1) WHERE FALSE"

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_perm_variables(self):
        try:
            model_variables: List[dict] = self._get_variables_metadata_from_standard_model(self.params.domain)

            # Filter variables by permissibility
            perm_model = [
                var for var in model_variables if self._get_allowed_variable_permissibility(var) == self.permissibility
            ]

            # Replace wildcards and extract variable names
            variable_names_list = self._replace_variable_wildcards(perm_model, self.params.domain)

            # Extract just the variable names from the processed metadata
            return variable_names_list

        except Exception as e:
            # If the metadata retrieval fails, the rule can't run, so throwing error
            raise Exception(f"Metadata retrieval failed due to error: {str(e)}")

    def _get_allowed_variable_permissibility(self, variable_metadata: dict):
        """
        Returns the permissibility value of a variable allowed in the current domain
        """
        variable_name = variable_metadata.get("name")
        if PERMISSIBILITY_KEY in variable_metadata:
            return variable_metadata[PERMISSIBILITY_KEY]
        elif variable_name in REQUIRED_MODEL_VARIABLES:
            return REQUIRED
        elif variable_name.replace("--", self.params.domain) == SEQ_VARIABLE.replace("--", self.params.domain):
            return REQUIRED

        return PERMISSIBLE
