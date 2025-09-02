from .base_sql_operator import BaseSqlOperator


class DateGreaterThanOrEqualToOperator(BaseSqlOperator):
    """Operator for date greater-than-or-equal-to comparisons."""

    def execute_operator(self, other_value):
        """Check if target date is greater than or equal to comparator date"""
        return self._date_comparison(other_value, ">=")
