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

        if self._is_collection(target):
            return self._route_collection_target(target, comparator, value_is_literal)

        target_column = self.replace_prefix(target)
        return self._route_column_target(target_column, comparator, value_is_literal)

    def _is_collection(self, value):
        return (
            isinstance(value, str)
            and value in self.operation_variables
            and self.operation_variables[value].type == "collection"
        )

    def _is_column(self, value):
        return isinstance(value, str) and self._exists(self.replace_prefix(value))

    def _route_collection_target(self, target, comparator, value_is_literal):
        if isinstance(comparator, list):
            return self._handle_collection_vs_list(target, comparator)
        elif value_is_literal:
            return self._handle_collection_vs_literal(target, comparator)
        elif self._is_collection(comparator):
            return self._handle_collection_vs_collection(target, comparator)
        elif self._is_column(comparator):
            return self._handle_collection_vs_column(target, comparator)

        raise ValueError(f"Unsupported comparator variant for collection target: " f"{type(comparator).__name__}")

    def _route_column_target(self, target_column, comparator, value_is_literal):
        if isinstance(comparator, list):
            return self._handle_list_comparator(target_column, comparator)
        elif value_is_literal:
            return self._handle_literal_value(target_column, comparator)
        elif isinstance(comparator, str) and comparator in self.operation_variables:
            return self._handle_operation_variable_comparator(target_column, comparator)
        elif self._is_column(comparator):
            return self._handle_column_comparator(target_column, comparator)
        elif isinstance(comparator, str):
            return self._handle_literal_value(target_column, comparator)

        return self._handle_invalid_comparator(target_column, comparator)

    def _handle_collection_vs_collection(self, target, comparator):
        def sql():
            target_sql = self._collection_sql(target)
            comparator_sql = self._collection_sql(comparator)

            return f"""EXISTS (
                SELECT 1
                FROM {comparator_sql} AS comp_vals(c_val)
                CROSS JOIN {target_sql} AS target_vals(t_val)
                WHERE comp_vals.c_val IS NOT NULL AND comp_vals.c_val != ''
                  AND target_vals.t_val IS NOT NULL AND target_vals.t_val != ''
                  AND comp_vals.c_val LIKE '%' || target_vals.t_val || '%'
            )"""

        return self._do_check_operator(sql)

    def _handle_collection_vs_literal(self, target_collection, comparator_literal):
        def sql():
            target_sql = self._collection_sql(target_collection, lowercase=self.case_insensitive)
            comparator_sql = self._constant_sql(comparator_literal, lowercase=self.case_insensitive)

            return f"""EXISTS (
                SELECT 1 FROM {target_sql} AS target_vals(t_val)
                WHERE target_vals.t_val IS NOT NULL AND target_vals.t_val != ''
                AND {comparator_sql} LIKE '%' || target_vals.t_val || '%'
            )"""

        return self._do_check_operator(sql)

    def _handle_collection_vs_column(self, target_collection, comparator_column):
        comparator_col_clean = self.replace_prefix(comparator_column)

        def sql():
            target_sql = self._collection_sql(target_collection, lowercase=self.case_insensitive)
            comparator_sql = self._column_sql(comparator_col_clean, lowercase=self.case_insensitive)

            return f"""NOT ({self._is_empty_sql(comparator_col_clean)})
                   AND EXISTS (
                       SELECT 1 FROM {target_sql} AS target_vals(t_val)
                       WHERE target_vals.t_val IS NOT NULL AND target_vals.t_val != ''
                       AND {comparator_sql} LIKE '%' || target_vals.t_val || '%'
                   )"""

        return self._do_check_operator(sql)

    def _handle_collection_vs_list(self, target_collection, comparator_list):
        def sql():
            target_sql = self._collection_sql(target_collection, lowercase=self.case_insensitive)
            values_sql = ", ".join(
                f"({self._constant_sql(v, lowercase=self.case_insensitive)})" for v in comparator_list
            )

            return f"""EXISTS (
                SELECT 1 FROM (VALUES {values_sql}) AS comp_vals(c_val)
                CROSS JOIN {target_sql} AS target_vals(t_val)
                WHERE comp_vals.c_val IS NOT NULL AND comp_vals.c_val != ''
                  AND target_vals.t_val IS NOT NULL AND target_vals.t_val != ''
                  AND comp_vals.c_val LIKE '%' || target_vals.t_val || '%'
            )"""

        return self._do_check_operator(sql)

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
            f"Expected list, column name, string literal, or operation variable, but got: "
            f"{type(comparator).__name__}"
        )
