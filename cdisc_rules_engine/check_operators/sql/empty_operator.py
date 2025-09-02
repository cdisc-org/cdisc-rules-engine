from .base_sql_operator import BaseSqlOperator


class EmptyOperator(BaseSqlOperator):
    """Operator for checking if values are empty/null."""

    def execute_operator(self, other_value):
        column = self.replace_prefix(other_value.get("target"))

        def sql():
            return self._is_empty_sql(column)

        return self._do_check_operator(f"{column}_empty", sql)
