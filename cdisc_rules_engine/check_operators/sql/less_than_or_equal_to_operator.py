from .base_sql_operator import BaseSqlOperator


class LessThanOrEqualToOperator(BaseSqlOperator):
    """Operator for numeric less-than-or-equal-to comparisons."""

    def execute_operator(self, other_value):
        return self._numeric_comparison(other_value, "<=")
