from .base_sql_operator import BaseSqlOperator


class LessThanOperator(BaseSqlOperator):
    """Operator for numeric less-than comparisons."""

    def execute_operator(self, other_value):
        return self._numeric_comparison(other_value, "<")
