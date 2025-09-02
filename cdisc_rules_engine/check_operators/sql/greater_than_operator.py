from .base_sql_operator import BaseSqlOperator


class GreaterThanOperator(BaseSqlOperator):
    """Operator for numeric greater-than comparisons."""

    def execute_operator(self, other_value):
        return self._numeric_comparison(other_value, ">")
