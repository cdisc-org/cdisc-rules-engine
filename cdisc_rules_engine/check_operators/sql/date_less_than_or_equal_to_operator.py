from .base_sql_operator import BaseSqlOperator


class DateLessThanOrEqualToOperator(BaseSqlOperator):
    """Operator for date less-than-or-equal-to comparisons."""

    def execute_operator(self, other_value):
        """Check if target date is less than or equal to comparator date"""
        return self._date_comparison(other_value, "<=")
