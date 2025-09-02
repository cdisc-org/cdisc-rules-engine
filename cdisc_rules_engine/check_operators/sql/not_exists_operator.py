from .base_sql_operator import BaseSqlOperator


class NotExistsOperator(BaseSqlOperator):
    """Operator for checking column non-existence."""

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target"))
        result = not self._exists(target_column)
        return self._do_check_operator(f"""{target_column}_notexists""", lambda: "TRUE" if result else "FALSE")
