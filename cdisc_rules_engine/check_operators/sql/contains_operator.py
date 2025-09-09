from .base_sql_operator import BaseSqlOperator


class ContainsOperator(BaseSqlOperator):
    """Operator for checking if target contains comparator values."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        """
        Checks if the comparator value is a substring of the target column values.
        Returns True if the comparator is found as a substring within the target column.
        """
        target_column = self.replace_prefix(other_value.get("target"))
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        if isinstance(comparator, str) and comparator in self.operation_variables:
            return self._handle_operation_variable_comparator(target_column, comparator)
        elif isinstance(comparator, list):
            return self._handle_list_comparator(target_column, comparator)
        elif value_is_literal:
            return self._handle_literal_value(target_column, comparator, value_is_literal)
        elif isinstance(comparator, str) and self._exists(comparator):
            return self._handle_column_comparator(target_column, comparator)
        elif isinstance(comparator, str):
            # String literals when not explicitly marked as literal
            return self._handle_literal_value(target_column, comparator, True)
        else:
            return self._handle_invalid_comparator(target_column, comparator)

    def _handle_list_comparator(self, target_column, comparator):
        """Handle when comparator is a list."""
        cache_key = f"{target_column}_contains_list_{self.case_insensitive}"

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            values_sql = ", ".join(f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in comparator)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM (VALUES {values_sql}) AS list_values(value)
                          WHERE list_values.value != ''
                          AND {target_sql} LIKE '%' || list_values.value || '%'
                      )"""

        return self._do_check_operator(cache_key, sql)

    def _handle_operation_variable_comparator(self, target_column, comparator):
        """Handle when comparator is an operation variable."""
        variable = self.operation_variables[comparator]
        cache_key = f"{target_column}_contains_opvar_{comparator}_{self.case_insensitive}"

        if variable.type == "constant":
            return self._handle_constant_variable(target_column, comparator, cache_key)
        elif variable.type == "collection":
            return self._handle_collection_variable(target_column, comparator, cache_key)
        else:
            raise ValueError(
                f"Unsupported operation variable type: {variable.type} "
                f"for variable {comparator}. Expected 'collection' or 'constant'."
            )

    def _handle_collection_variable(self, target_column, comparator, cache_key):
        """Handle collection type operation variable."""

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            collection_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM {collection_sql} AS collection_values(value)
                          WHERE collection_values.value != ''
                          AND {target_sql} LIKE '%' || collection_values.value || '%'
                      )"""

        return self._do_check_operator(cache_key, sql)

    def _handle_constant_variable(self, target_column, comparator, cache_key):
        """Handle constant type operation variable."""

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} != ''
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return self._do_check_operator(cache_key, sql)

    def _handle_column_comparator(self, target_column, comparator):
        """Handle when comparator is a column name."""
        comparator_column = self.replace_prefix(comparator)
        cache_key = f"{target_column}_contains_{comparator_column}_{self.case_insensitive}"

        def sql():
            comparator_sql = self._column_sql(comparator, lowercase=self.case_insensitive)
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND NOT ({self._is_empty_sql(comparator)})
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return self._do_check_operator(cache_key, sql)

    def _handle_literal_value(self, target_column, comparator, value_is_literal):
        """Handle single literal value case."""
        cache_key = f"{target_column}_contains_literal_{comparator}_{value_is_literal}_{self.case_insensitive}"

        def sql():
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} != ''
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return self._do_check_operator(cache_key, sql)

    def _handle_invalid_comparator(self, target_column, comparator):
        """Handle invalid comparator types."""
        raise ValueError(
            f"Invalid comparator type for contains operation on column '{target_column}'. "
            f"Expected list, column name, or operation variable, but got: {type(comparator).__name__}"
        )
