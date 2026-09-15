from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from typing import List


class SqlGetColumnOrderFromLibrary(SqlBaseOperation):

    def _execute_operation(self):
        """
        Fetches column order for a given domain from the CDISC library.

        The list of column names is sorted in accordance with the "ordinal" key of the
        library metadata. Optionally filters variables based on specified metadata criteria
        (key_name/key_value, e.g. key_name="role", key_value="Timing") before the names are
        extracted.
        """
        library_variables = self._get_library_column_order_variables()

        query = self._format_variable_list_to_query(vars=library_variables)

        return SqlOperationResult(query=query, type="collection", subtype="Char")

    def _get_library_column_order_variables(self) -> List[str]:
        try:
            # the variables are sorted according to their ordinal value in the library metadata in this method
            variables_metadata: List[dict] = self._get_variables_metadata_from_standard(self.params.domain)

            variables_metadata = self._filter_by_metadata_criteria(variables_metadata)

            variable_names_list = self._replace_variable_wildcards(variables_metadata, self.params.domain)

            return variable_names_list

        except Exception as e:
            # If the metadata retrieval fails, the rule can't run, so throwing error
            raise Exception(f"Metadata retrieval failed due to error: {str(e)}")

    def _filter_by_metadata_criteria(self, variables_metadata: List[dict]) -> List[dict]:
        """Optionally filter variables by a key_name/key_value metadata criterion."""
        key = self.params.key_name
        val = self.params.key_value
        if not key or not val:
            return variables_metadata

        return [var for var in variables_metadata if var.get(key) == val]
