from .base_sql_operator import BaseSqlOperator


class GreaterThanOrEqualToOperator(BaseSqlOperator):
    """Operator for numeric greater-than-or-equal-to comparisons."""

    def execute_operator(self, other_value):
        return self._numeric_comparison(other_value, ">=")
