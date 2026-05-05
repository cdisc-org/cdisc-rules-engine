from .base_sql_operator import BaseSqlOperator


class ContainsOperator(BaseSqlOperator):
    """Operator for checking if target contains comparator values."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        """
        Checks if the comparator value is a substring of the target column values.
        Also handles cases where the target is a collection operation variable.
        Returns True if the comparator is found as a substring within the target column.
        """
        target = other_value.get("target")
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        if target in self.operation_variables:
            target_var = self.operation_variables[target]
            if target_var.type == "collection" and value_is_literal:
                return self._handle_target_is_collection(target, comparator)

        target_column = self.replace_prefix(target)

        if isinstance(comparator, str) and comparator in self.operation_variables:
            return self._handle_operation_variable_comparator(target_column, comparator)
        elif isinstance(comparator, list):
            return self._handle_list_comparator(target_column, comparator)
        elif value_is_literal:
            return self._handle_literal_value(target_column, comparator)
        elif isinstance(comparator, str) and self._exists(comparator):
            return self._handle_column_comparator(target_column, comparator)
        elif isinstance(comparator, str):
            # String literals when not explicitly marked as literal
            return self._handle_literal_value(target_column, comparator)
        else:
            return self._handle_invalid_comparator(target_column, comparator)

    def _handle_list_comparator(self, target_column, comparator):
        """Handle when comparator is a list."""

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            values_sql = ", ".join(f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in comparator)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM (VALUES {values_sql}) AS list_values(value)
                          WHERE list_values.value != ''
                          AND {target_sql} LIKE '%' || list_values.value || '%'
                      )"""

        return self._do_check_operator(sql)

    def _handle_operation_variable_comparator(self, target_column, comparator):
        """Handle when comparator is an operation variable."""
        variable = self.operation_variables[comparator]

        if variable.type == "constant":
            return self._handle_constant_variable(target_column, comparator)
        elif variable.type == "collection":
            return self._handle_collection_variable(target_column, comparator)
        else:
            raise ValueError(
                f"Unsupported operation variable type: {variable.type} "
                f"for variable {comparator}. Expected 'collection' or 'constant'."
            )

    def _handle_collection_variable(self, target_column, comparator):
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

        return self._do_check_operator(sql)

    def _handle_constant_variable(self, target_column, comparator):
        """Handle constant type operation variable."""

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} != ''
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return self._do_check_operator(sql)

    def _handle_column_comparator(self, target_column, comparator):
        """Handle when comparator is a column name."""
        comparator_column = self.replace_prefix(comparator)

        def sql():
            comparator_sql = self._column_sql(comparator_column, lowercase=self.case_insensitive)
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND NOT ({self._is_empty_sql(comparator_column)})
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return self._do_check_operator(sql)

    def _handle_literal_value(self, target_column, comparator):
        """Handle single literal value case."""

        def sql():
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} != ''
                      AND {target_sql} LIKE '%' || {comparator_sql} || '%'"""

        return self._do_check_operator(sql)

    def _handle_invalid_comparator(self, target_column, comparator):
        """Handle invalid comparator types."""
        raise ValueError(
            f"Invalid comparator type for contains operation on column '{target_column}'. "
            f"Expected list, column name, or operation variable, but got: {type(comparator).__name__}"
        )

    def _handle_target_is_collection(self, target_variable, comparator, value_is_literal):
        """Handle when the target is a collection operation variable."""

        def sql():
            collection_sql = self._collection_sql(target_variable, lowercase=self.case_insensitive)
            if value_is_literal:
                comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            else:
                comparator_sql = self._sql(comparator, lowercase=self.case_insensitive)

            return f"""EXISTS (
                        SELECT 1 FROM {collection_sql} AS collection_values(value)
                        WHERE collection_values.value = {comparator_sql}
                    )"""

        return self._do_check_operator(sql)
