from .base_sql_operator import BaseSqlOperator


class ExistsOperator(BaseSqlOperator):
    """Operator for checking column existence."""

    def _execute_operator_impl(self, other_value):
        target_column = self.replace_prefix(other_value.get("target"))
        result = target_column in self.operation_variables or self._exists(target_column)
        return self._do_check_operator(f"{target_column}_exists", lambda: "TRUE" if result else "FALSE")

    def get_result_for_missing_columns(self):
        return "FALSE"  # matches the behaviour above but method is required for consistency
