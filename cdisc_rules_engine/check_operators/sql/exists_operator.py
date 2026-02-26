from .base_sql_operator import BaseSqlOperator


class ExistsOperator(BaseSqlOperator):
    """Operator for checking column existence."""

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target"))
        result = target_column in self.operation_variables or self._exists(target_column)
        return self._do_check_operator(lambda: "TRUE" if result else "FALSE")
