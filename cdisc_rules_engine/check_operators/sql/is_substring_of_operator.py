from .base_sql_operator import BaseSqlOperator


class IsSubstringOfOperator(BaseSqlOperator):
    """Checks if the target column value is a substring of the comparator values."""

    def __init__(self, data, case_insensitive=False):
        super().__init__(data)
        self.case_insensitive = case_insensitive

    def execute_operator(self, other_value):
        target = other_value.get("target")
        value_is_literal = other_value.get("value_is_literal", False)
        comparator = other_value.get("comparator")

        target_column = self.replace_prefix(target)

        if isinstance(comparator, str) and not value_is_literal and comparator in self.operation_variables:
            return self._handle_operation_variable_comparator(target_column, comparator)
        elif isinstance(comparator, list):
            return self._handle_list_comparator(target_column, comparator)
        elif value_is_literal:
            return self._handle_literal_value(target_column, comparator)
        elif isinstance(comparator, str) and self._exists(self.replace_prefix(comparator)):
            return self._handle_column_comparator(target_column, comparator)
        elif isinstance(comparator, str):
            return self._handle_literal_value(target_column, comparator)
        else:
            return self._handle_invalid_comparator(target_column, comparator)

    def _handle_list_comparator(self, target_column, comparator):
        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            values_sql = ", ".join(f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in comparator)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM (VALUES {values_sql}) AS list_values(value)
                          WHERE list_values.value IS NOT NULL
                          AND list_values.value != ''
                          AND list_values.value LIKE '%' || {target_sql} || '%'
                      )"""

        return self._do_check_operator(sql)

    def _handle_operation_variable_comparator(self, target_column, comparator):
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
        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            collection_sql = self._collection_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND EXISTS (
                          SELECT 1 FROM {collection_sql} AS collection_values(value)
                          WHERE collection_values.value IS NOT NULL
                          AND collection_values.value != ''
                          AND collection_values.value LIKE '%' || {target_sql} || '%'
                      )"""

        return self._do_check_operator(sql)

    def _handle_constant_variable(self, target_column, comparator):
        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} IS NOT NULL
                      AND {comparator_sql} != ''
                      AND {comparator_sql} LIKE '%' || {target_sql} || '%'"""

        return self._do_check_operator(sql)

    def _handle_column_comparator(self, target_column, comparator):
        comparator_column = self.replace_prefix(comparator)

        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            comparator_sql = self._column_sql(comparator_column, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} IS NOT NULL
                      AND {comparator_sql} != ''
                      AND {comparator_sql} LIKE '%' || {target_sql} || '%'"""

        return self._do_check_operator(sql)

    def _handle_literal_value(self, target_column, comparator):
        def sql():
            target_sql = self._column_sql(target_column, lowercase=self.case_insensitive)
            comparator_sql = self._constant_sql(comparator, lowercase=self.case_insensitive)
            return f"""NOT ({self._is_empty_sql(target_column)})
                      AND {comparator_sql} IS NOT NULL
                      AND {comparator_sql} != ''
                      AND {comparator_sql} LIKE '%' || {target_sql} || '%'"""

        return self._do_check_operator(sql)

    def _handle_invalid_comparator(self, target_column, comparator):
        raise ValueError(
            f"Invalid comparator type for is_substring_of operation on column '{target_column}'. "
            f"Expected list, column name, string literal, or operation variable, but got: {type(comparator).__name__}"
        )
